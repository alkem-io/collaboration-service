package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	ycrdt "github.com/antst/go-yjs/crdt"
)

func TestMemoMarkdownMatchesPinnedServer(t *testing.T) {
	body, err := os.ReadFile("testdata/memo-markdown/oracle.json")
	if err != nil {
		t.Fatal(err)
	}
	var samples []struct {
		Name     string
		Snapshot []byte
		Markdown string
	}
	if err := json.Unmarshal(body, &samples); err != nil || len(samples) < 5 {
		t.Fatalf("memo oracle: got %d samples, err=%v", len(samples), err)
	}
	for _, sample := range samples {
		t.Run(sample.Name, func(t *testing.T) {
			doc := ycrdt.NewDoc("memo-oracle")
			defer doc.Destroy()
			if err := ycrdt.ApplyUpdateV2(doc, sample.Snapshot, nil); err != nil {
				t.Fatal(err)
			}
			got, err := serializeMemoMarkdown(t.Context(), doc, memoMarkdownLimits{depth: 32, nodes: 1024, bytes: 4096})
			if err != nil || got != sample.Markdown {
				t.Fatalf("memo differs from pinned serializer, err=%v\n got: %q\nwant: %q", err, got, sample.Markdown)
			}
			// Every tighter output limit must either preserve the complete
			// reference bytes or fail without a partial preview. This also
			// covers limits between list prefixes, table cells and embed attributes.
			for limit := 0; limit < len(sample.Markdown); limit++ {
				body, err := serializeMemoMarkdown(t.Context(), doc, memoMarkdownLimits{depth: 32, nodes: 1024, bytes: limit})
				if !errors.Is(err, ErrSavedContent) || body != "" {
					t.Fatalf("output limit %d leaked a partial result: %q, %v", limit, body, err)
				}
			}
			checkMemoTraversalBudgets(t, doc, sample.Markdown)
		})
	}
}

// A tighter traversal limit can refuse the document, but must never silently
// omit its deepest or last node.
func checkMemoTraversalBudgets(t *testing.T, doc *ycrdt.Doc, expected string) {
	t.Helper()
	for limit := range 40 {
		for _, limits := range []memoMarkdownLimits{
			{depth: 32, nodes: limit, bytes: 4096},
			{depth: limit, nodes: 1024, bytes: 4096},
		} {
			body, err := serializeMemoMarkdown(t.Context(), doc, limits)
			if err == nil && body == expected || errors.Is(err, ErrSavedContent) && body == "" {
				continue
			}
			t.Fatalf("traversal limits %+v changed the memo: %q, %v", limits, body, err)
		}
	}
}

func TestMemoMarkdownUnsupportedNestedContentNeverReturnsPartialMarkdown(t *testing.T) {
	for _, tc := range []struct {
		path string
		leaf string
		attr string
	}{
		{"", "$text", ""},
		{"codeBlock", "hardBreak", ""},
		{"paragraph", "image/paragraph", ""},
		{"blockquote/bulletList", "paragraph", ""},
		{"blockquote/bulletList/listItem/paragraph", "missing-inline-node", ""},
		{"blockquote/orderedList/listItem/codeBlock", "image", ""},
		{"orderedList/listItem", "$text", ""},
		{"bulletList/listItem", "unsupported", ""},
		{"bulletList/listItem", "orderedList", ""},
		{"", "table", ""},
		{"table", "tableRow", ""},
		{"table/tableRow", "paragraph", ""},
		{"table/tableRow/tableCell", "$text", ""},
		{"table/tableRow/tableCell/paragraph", "unsupported", ""},
		{"blockquote/table/tableRow/tableCell", "unsupported", ""},
		{"", "horizontalRule/paragraph", ""},
		{"", "iframe/paragraph", ""},
		{"", "iframe", "scale"},
		{"paragraph", "$link", ""},
		{"paragraph", "$duplicate-mark", ""},
	} {
		t.Run(tc.path+"/"+tc.leaf, func(t *testing.T) {
			doc := ycrdt.NewDoc("bad-nested-memo")
			defer doc.Destroy()
			root := doc.GetXMLFragment(memoRoot)
			p := ycrdt.NewYXmlElement("paragraph")
			root.Push(ycrdt.ArrayAny{p})
			content := ycrdt.NewYXmlText()
			p.Push(ycrdt.ArrayAny{content})
			content.Insert(0, "must not leak", ycrdt.Object{})
			addBadMemoPath(root, strings.TrimPrefix(tc.path+"/"+tc.leaf, "/"), tc.attr)
			md, err := renderSavedMemo(t.Context(), doc)
			if !errors.Is(err, ErrSavedContent) || md != "" {
				t.Fatalf("invalid nested memo should fail closed; got %q, %v", md, err)
			}
		})
	}
}

func addBadMemoPath(root interface{ Push(content ycrdt.ArrayAny) }, path, attr string) {
	for name := range strings.SplitSeq(path, "/") {
		if strings.HasPrefix(name, "$") {
			text := ycrdt.NewYXmlText()
			root.Push(ycrdt.ArrayAny{text})
			marks := ycrdt.Object{}
			switch name {
			case "$link":
				marks = ycrdt.MakeObject("link", 42)
			case "$duplicate-mark":
				marks = ycrdt.MakeObject("bold", ycrdt.Object{}, "bold--AAAAAAAA", ycrdt.Object{})
			}
			text.Insert(0, "invalid", marks)
			return
		}
		node := ycrdt.NewYXmlElement(name)
		root.Push(ycrdt.ArrayAny{node})
		if attr != "" {
			node.SetAttribute("src", ycrdt.MakeObject(attr, 12))
		}
		root = node
	}
}

func TestMemoMarkdownRejectsMalformedAndUnboundedTraversal(t *testing.T) {
	for _, tt := range []struct {
		name   string
		setup  func(*ycrdt.YXmlFragment)
		limits memoMarkdownLimits
		cancel bool
		want   error
	}{
		{"unknown block", func(root *ycrdt.YXmlFragment) {
			root.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("unsupported")})
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"unknown inline node after good text", func(root *ycrdt.YXmlFragment) {
			p := ycrdt.NewYXmlElement("paragraph")
			root.Push(ycrdt.ArrayAny{p})
			x := ycrdt.NewYXmlText()
			p.Insert(0, ycrdt.ArrayAny{x, ycrdt.NewYXmlElement("unsupported")})
			x.Insert(0, "must never escape in a partial response", ycrdt.Object{})
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"non-heading level", func(root *ycrdt.YXmlFragment) {
			h := ycrdt.NewYXmlElement("heading")
			root.Push(ycrdt.ArrayAny{h})
			h.SetAttribute("level", 7)
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"unsupported mark", func(root *ycrdt.YXmlFragment) {
			p := ycrdt.NewYXmlElement("paragraph")
			root.Push(ycrdt.ArrayAny{p})
			x := ycrdt.NewYXmlText()
			p.Insert(0, ycrdt.ArrayAny{x})
			x.Insert(0, "not unformatted", ycrdt.MakeObject("unsupported", ycrdt.Object{}))
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"empty list is not one valid empty memo", func(root *ycrdt.YXmlFragment) {
			root.Push(ycrdt.ArrayAny{ycrdt.NewYXmlElement("orderedList")})
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"invalid table row", func(root *ycrdt.YXmlFragment) {
			table := ycrdt.NewYXmlElement("table")
			root.Push(ycrdt.ArrayAny{table})
			table.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("tableCell")})
		}, memoMarkdownLimits{10, 20, 100}, false, ErrSavedContent},
		{"bad iframe attribute", func(root *ycrdt.YXmlFragment) {
			iframe := ycrdt.NewYXmlElement("iframe")
			root.Push(ycrdt.ArrayAny{iframe})
			iframe.SetAttribute("src", "a\"b")
		}, memoMarkdownLimits{10, 20, 1024}, false, ErrSavedContent},
		{"depth", func(root *ycrdt.YXmlFragment) {
			quote := ycrdt.NewYXmlElement("blockquote")
			root.Insert(0, ycrdt.ArrayAny{quote})
			quote.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph")})
		}, memoMarkdownLimits{1, 20, 100}, false, ErrSavedContent},
		{"work", func(root *ycrdt.YXmlFragment) {
			root.Insert(0, ycrdt.ArrayAny{ycrdt.NewYXmlElement("paragraph"), ycrdt.NewYXmlElement("paragraph")})
		}, memoMarkdownLimits{10, 1, 100}, false, ErrSavedContent},
		{"rendered bytes", func(root *ycrdt.YXmlFragment) {
			node := ycrdt.NewYXmlElement("paragraph")
			root.Insert(0, ycrdt.ArrayAny{node})
			ytext := ycrdt.NewYXmlText()
			node.Insert(0, ycrdt.ArrayAny{ytext})
			ytext.Insert(0, "saved text", ycrdt.Object{})
		}, memoMarkdownLimits{10, 20, 4}, false, ErrSavedContent},
		{"canceled", func(*ycrdt.YXmlFragment) {}, memoMarkdownLimits{10, 20, 100}, true, context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc := ycrdt.NewDoc("memo-limits")
			defer doc.Destroy()
			tt.setup(doc.GetXMLFragment(memoRoot))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancel {
				cancel()
			}
			got, err := serializeMemoMarkdown(ctx, doc, tt.limits)
			if !errors.Is(err, tt.want) || got != "" {
				t.Fatalf("got %q, %v; want no bytes and %v", got, err, tt.want)
			}
		})
	}
}
