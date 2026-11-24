package service

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/antst/go-yjs/backend/persistence"
	ycrdt "github.com/antst/go-yjs/crdt"

	"github.com/alkem-io/collaboration-service/internal/domain/model"
	"github.com/alkem-io/collaboration-service/internal/domain/port"
)

// NewMemoMarkdownReader constructs a saved-content read, independent of rooms.
func NewMemoMarkdownReader(metadata port.MetadataStore, checkpoints persistence.CheckpointStore) func(context.Context, model.DocumentID) (string, error) {
	source := savedProjection{metadata: metadata, checkpoints: checkpoints, maxBytes: PreviewSnapshotLimit}
	return func(ctx context.Context, id model.DocumentID) (string, error) {
		doc, err := source.open(ctx, id, model.ContentTypeMemo)
		if err != nil {
			return "", err
		}
		defer doc.Destroy()
		return renderSavedMemo(ctx, doc)
	}
}

func renderSavedMemo(ctx context.Context, doc *ycrdt.Doc) (string, error) {
	return serializeMemoMarkdown(ctx, doc, memoMarkdownLimits{depth: 32, nodes: 16 << 10, bytes: 2 << 20})
}

const memoBlankLine = "\u00a0"

type memoMarkdownLimits struct{ depth, nodes, bytes int }

type memoMarkdown struct {
	ctx    context.Context
	limits memoMarkdownLimits
	seen   int
}

func serializeMemoMarkdown(ctx context.Context, doc *ycrdt.Doc, limits memoMarkdownLimits) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	root := doc.GetXMLFragment(memoRoot)
	if root == nil || root.GetLength() > limits.nodes {
		return "", ErrSavedContent
	}
	m := memoMarkdown{ctx: ctx, limits: limits}
	result, err := m.blocks(root.ToArray(), 1, "\n\n", true)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return trimECMAScript(result), nil
}

func (m *memoMarkdown) visit(depth, children int) error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	m.seen++
	if depth > m.limits.depth || children > m.limits.nodes-m.seen {
		return ErrSavedContent
	}
	return nil
}

func (m *memoMarkdown) append(out *strings.Builder, parts ...string) error {
	for _, part := range parts {
		if len(part) > m.limits.bytes-out.Len() {
			return ErrSavedContent
		}
		out.WriteString(part)
	}
	return nil
}

func (m *memoMarkdown) blocks(children ycrdt.ArrayAny, depth int, sep string, root bool) (string, error) {
	var out strings.Builder
	for i, child := range children {
		block, ok := child.(*ycrdt.YXmlElement)
		if !ok {
			return "", ErrSavedContent
		}
		text, err := m.block(block, depth, root)
		if err != nil {
			return "", err
		}
		if i > 0 {
			if err := m.append(&out, sep); err != nil {
				return "", err
			}
		}
		if err := m.append(&out, text); err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

// root selects the server's outer serializer. Nested blocks use the pinned
// TipTap defaults except in lists, where the server owns nesting/indentation.
func (m *memoMarkdown) block(node *ycrdt.YXmlElement, depth int, root bool) (string, error) {
	if err := m.visit(depth, node.GetLength()); err != nil {
		return "", err
	}
	children, attrs := node.ToArray(), node.GetAttributes()
	switch node.NodeName {
	case "paragraph", "heading", "codeBlock":
		return m.textBlock(node.NodeName, children, attrs, depth+1, root)
	case "bulletList", "orderedList":
		return m.list(children, node.NodeName == "orderedList", root, "", depth+1)
	case "table":
		return m.table(children, depth+1, root)
	case "blockquote":
		body, err := m.blocks(children, depth+1, "", false)
		if err != nil {
			return "", err
		}
		var out strings.Builder
		// TipTap prefixes every line, including blank lines in a quote.
		for line := range strings.SplitSeq(trimECMAScript(body), "\n") {
			if err := m.append(&out, "\n> ", line); err != nil {
				return "", err
			}
		}
		return out.String(), nil
	case "horizontalRule":
		if len(children) == 0 {
			return "\n---\n", nil
		}
	case "iframe":
		if len(children) == 0 {
			return m.iframe(attrs)
		}
	}
	return "", ErrSavedContent
}

func (m *memoMarkdown) textBlock(kind string, children ycrdt.ArrayAny, attrs ycrdt.Object, depth int, root bool) (string, error) {
	body, placeholder, err := m.inline(children, depth, kind == "codeBlock")
	if err != nil {
		return "", err
	}
	switch kind {
	case "heading":
		level, err := strconv.Atoi(memoAttribute(attrs, "level", "1"))
		if err != nil || level < 1 || level > 6 {
			return "", ErrSavedContent
		}
		return strings.Repeat("#", level) + " " + body + "\n", nil
	case "codeBlock":
		return "\n```" + memoAttribute(attrs, "language", "null") + "\n" + body + "\n```\n", nil
	default:
		if root && (len(children) == 0 || placeholder) {
			return memoBlankLine, nil
		}
		return "\n" + body + "\n", nil
	}
}

func (m *memoMarkdown) inline(children ycrdt.ArrayAny, depth int, code bool) (string, bool, error) {
	var out strings.Builder
	placeholder := false
	for _, child := range children {
		if err := m.visit(depth, 0); err != nil {
			return "", false, err
		}
		switch node := child.(type) {
		case *ycrdt.YXmlText:
			ops := node.ToDelta(nil, nil, nil)
			placeholder = len(children) == 1 && len(ops) == 1 && ops[0].InsertText == "<br>"
			for _, op := range ops {
				if err := m.textRun(&out, depth, code, op); err != nil {
					return "", false, err
				}
			}
		case *ycrdt.YXmlElement:
			if code {
				return "", false, ErrSavedContent
			}
			if err := m.inlineElement(&out, node); err != nil {
				return "", false, err
			}
		default:
			return "", false, ErrSavedContent
		}
	}
	return out.String(), placeholder, nil
}

func (m *memoMarkdown) textRun(out *strings.Builder, depth int, code bool, op ycrdt.EventOperator) error {
	if err := m.visit(depth, 0); err != nil {
		return err
	}
	if op.GetKind() != ycrdt.EventOperatorInsertText {
		return ErrSavedContent
	}
	if code || !op.HasAttributes() {
		return m.append(out, op.InsertText)
	}
	text, err := memoMarks(op.InsertText, op.Attributes)
	if err != nil {
		return err
	}
	return m.append(out, text)
}

func (m *memoMarkdown) inlineElement(out *strings.Builder, node *ycrdt.YXmlElement) error {
	if node.GetLength() != 0 {
		return ErrSavedContent
	}
	switch node.NodeName {
	case "hardBreak":
		return m.append(out, "\n")
	case "image":
		a := node.GetAttributes()
		return m.append(out, "![", memoAttribute(a, "alt", "null"), "](", memoAttribute(a, "src", "null"), ")")
	default:
		return ErrSavedContent
	}
}

var memoMarkSuffix = regexp.MustCompile(`--[a-zA-Z0-9+/=]{8}$`)

func memoMarks(text string, attrs ycrdt.Object) (string, error) {
	// ProseMirror applies marks in schema order, not in the on-wire key order.
	const link, bold, code, italic, strike, highlight = 0, 1, 2, 3, 4, 5
	names := [...]string{"link", "bold", "code", "italic", "strike", "highlight"}
	var values [6]any
	var seen [6]bool
	for _, key := range attrs.Keys() {
		i := slices.Index(names[:], memoMarkSuffix.ReplaceAllString(key, ""))
		if i < 0 || seen[i] {
			return "", ErrSavedContent
		}
		seen[i], values[i] = true, attrs.GetOr(key)
	}
	if seen[link] {
		obj, ok := values[link].(ycrdt.Object)
		if !ok {
			return "", ErrSavedContent
		}
		text = "[" + text + "](" + memoAttribute(obj, "href", "null") + ")"
	}
	if seen[bold] {
		text = "**" + text + "**"
	}
	if seen[code] {
		text = "`" + text + "`"
	}
	if seen[italic] {
		em := "_"
		if seen[bold] {
			em = "*"
		}
		text = em + text + em
	}
	if seen[strike] {
		text = "~~" + text + "~~"
	}
	if seen[highlight] {
		text = "==" + text + "=="
	}
	return text, nil
}

func memoAttribute(attrs ycrdt.Object, key, missing string) string {
	if v, ok := attrs.Get(key); ok && v != nil {
		return fmt.Sprint(v)
	}
	return missing
}

func (m *memoMarkdown) list(items ycrdt.ArrayAny, ordered, custom bool, indent string, depth int) (string, error) {
	if len(items) == 0 {
		return "", ErrSavedContent
	}
	var out strings.Builder
	if !custom {
		out.WriteByte('\n')
	}
	for i, child := range items {
		item, ok := child.(*ycrdt.YXmlElement)
		if !ok || item.NodeName != "listItem" {
			return "", ErrSavedContent
		}
		if err := m.visit(depth, item.GetLength()); err != nil {
			return "", err
		}
		marker := memoListMarker(i, ordered, custom)
		if custom {
			body, err := m.listItem(item.ToArray(), indent, marker, depth+1)
			if err != nil {
				return "", err
			}
			sep := ""
			if i > 0 {
				sep = "\n"
			}
			if err := m.append(&out, sep, body); err != nil {
				return "", err
			}
			continue
		}
		body, err := m.blocks(item.ToArray(), depth+1, "", false)
		if err != nil {
			return "", err
		}
		if err := m.append(&out, marker, " ", trimECMAScript(body), "\n"); err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

func memoListMarker(index int, ordered, custom bool) string {
	if !ordered {
		return "-"
	}
	if custom {
		return "1."
	}
	return strconv.Itoa(index+1) + "."
}

func (m *memoMarkdown) listItem(children ycrdt.ArrayAny, indent, marker string, depth int) (string, error) {
	var out strings.Builder
	if err := m.append(&out, indent, marker); err != nil {
		return "", err
	}
	continuation := indent + strings.Repeat(" ", len(marker)+1)
	for i, child := range children {
		node, ok := child.(*ycrdt.YXmlElement)
		if !ok {
			return "", ErrSavedContent
		}
		if i != 0 {
			if err := m.append(&out, "\n\n"); err != nil {
				return "", err
			}
		}
		if node.NodeName == "orderedList" || node.NodeName == "bulletList" {
			if err := m.visit(depth, node.GetLength()); err != nil {
				return "", err
			}
			if i == 0 {
				if err := m.append(&out, "\n"); err != nil {
					return "", err
				}
			}
			body, err := m.list(node.ToArray(), node.NodeName == "orderedList", true, continuation, depth+1)
			if err != nil {
				return "", err
			}
			if err := m.append(&out, body); err != nil {
				return "", err
			}
			continue
		}
		if err := m.listItemBlock(&out, node, depth, continuation, i == 0); err != nil {
			return "", err
		}
	}
	return out.String(), nil
}

func (m *memoMarkdown) listItemBlock(out *strings.Builder, node *ycrdt.YXmlElement, depth int, continuation string, first bool) error {
	body, err := m.block(node, depth, false)
	if err != nil {
		return err
	}
	body = trimECMAScript(body)
	if body == "" && node.NodeName == "paragraph" {
		body = memoBlankLine
	}
	prefix := continuation
	if first {
		prefix = " "
	}
	for line := range strings.SplitSeq(body, "\n") {
		if err := m.append(out, prefix, line); err != nil {
			return err
		}
		prefix = "\n" + continuation
	}
	return nil
}

func (m *memoMarkdown) table(rows ycrdt.ArrayAny, depth int, root bool) (string, error) {
	if len(rows) == 0 {
		return "", ErrSavedContent
	}
	var out strings.Builder
	if !root {
		out.WriteByte('\n')
	}
	for i, child := range rows {
		row, ok := child.(*ycrdt.YXmlElement)
		if !ok || row.NodeName != "tableRow" || row.GetLength() == 0 {
			return "", ErrSavedContent
		}
		if err := m.tableRow(&out, row, depth, root); err != nil {
			return "", err
		}
		if i == 0 {
			divider := " --- |"
			if root {
				divider = " ---- |"
			}
			if err := m.append(&out, "|", strings.Repeat(divider, row.GetLength()), "\n"); err != nil {
				return "", err
			}
		}
	}
	if root {
		return strings.TrimRight(out.String(), "\n"), nil
	}
	return out.String() + "\n", nil
}

func (m *memoMarkdown) tableRow(out *strings.Builder, row *ycrdt.YXmlElement, depth int, root bool) error {
	if err := m.visit(depth, row.GetLength()); err != nil {
		return err
	}
	if err := m.append(out, "|"); err != nil {
		return err
	}
	for _, child := range row.ToArray() {
		cell, ok := child.(*ycrdt.YXmlElement)
		if !ok || (cell.NodeName != "tableCell" && cell.NodeName != "tableHeader") {
			return ErrSavedContent
		}
		if err := m.visit(depth+1, cell.GetLength()); err != nil {
			return err
		}
		body, err := m.cell(cell.ToArray(), depth+2, root)
		if err != nil {
			return err
		}
		if err := m.append(out, " ", body, " |"); err != nil {
			return err
		}
	}
	return m.append(out, "\n")
}

func (m *memoMarkdown) cell(children ycrdt.ArrayAny, depth int, root bool) (string, error) {
	if !root {
		text, err := m.blocks(children, depth, "", false)
		return trimECMAScript(text), err
	}
	var out strings.Builder
	for _, child := range children {
		paragraph, ok := child.(*ycrdt.YXmlElement)
		if !ok {
			return "", ErrSavedContent
		}
		if err := m.visit(depth, paragraph.GetLength()); err != nil {
			return "", err
		}
		// The server's custom table serializer concatenates only paragraphs.
		if paragraph.NodeName != "paragraph" {
			continue
		}
		text, placeholder, err := m.inline(paragraph.ToArray(), depth+1, false)
		if err != nil {
			return "", err
		}
		if !placeholder {
			if err := m.append(&out, trimECMAScript(text)); err != nil {
				return "", err
			}
		}
	}
	if out.Len() == 0 {
		return " ", nil
	}
	return out.String(), nil
}

func (m *memoMarkdown) iframe(attrs ycrdt.Object) (string, error) {
	const wrapper = `<div class="iframe-wrapper" style="position: relative; height: 442px; width: 100%;"><iframe`
	var out strings.Builder
	if err := m.append(&out, wrapper); err != nil {
		return "", err
	}
	for _, a := range []struct {
		key      string
		fallback any
	}{
		{"src", nil}, {"position", "absolute"}, {"top", 0}, {"left", 0},
		{"width", "100%"}, {"height", "100%"}, {"frameborder", 0}, {"allowFullScreen", 1}, {"allowfullscreen", true},
	} {
		value, ok := attrs.Get(a.key)
		if !ok {
			value = a.fallback
		}
		var text string
		switch v := value.(type) {
		case nil:
			text = "null"
		case string:
			// Keep the oracle's quoted attributes without allowing saved text to
			// close an attribute and write new HTML markup.
			if strings.ContainsAny(v, "\"<>\x00\r\n") {
				return "", ErrSavedContent
			}
			text = `"` + v + `"`
		case bool, int, int64, float64:
			text = fmt.Sprint(v)
		default:
			return "", ErrSavedContent
		}
		if err := m.append(&out, " ", a.key, "=", text); err != nil {
			return "", err
		}
	}
	if err := m.append(&out, `/></div>`); err != nil {
		return "", err
	}
	return out.String(), nil
}
