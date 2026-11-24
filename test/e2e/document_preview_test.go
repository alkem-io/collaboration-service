//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ycrdt "github.com/antst/go-yjs/crdt"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/alkem-io/collaboration-service/internal/app"
	"github.com/alkem-io/collaboration-service/internal/domain/model"
)

func TestDocumentPreviewsPublishToStableRealFileRows(t *testing.T) {
	fsURL := os.Getenv("COLLAB_TEST_PREVIEW_FILE_SERVICE")
	if fsURL == "" {
		t.Skip("requires disposable real file-service on loopback")
	}
	u, err := url.Parse(fsURL)
	if err != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() {
		t.Fatal("preview fixture may only use an explicit local test file-service address")
	}
	board, memo, bucket := uuid.NewString(), uuid.NewString(), uuid.NewString()
	ref := "collab-preview-v1:" + board + ":"
	memoRef := "collab-preview-v1:" + memo + ":markdown"
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(ctx context.Context, method, path, media string, body io.Reader) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, fsURL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", media)
		req.Header.Set("X-Alkemio-Actor-Id", uuid.Nil.String())
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, b
	}
	call := func(method, path, media string, body io.Reader) (int, []byte) {
		t.Helper()
		return request(t.Context(), method, path, media, body)
	}
	find := func(ctx context.Context, ref string) string {
		t.Helper()
		status, body := request(ctx, http.MethodGet, "/internal/file/by-reference?"+url.Values{"ref": {ref}, "bucketId": {bucket}}.Encode(), "", nil)
		var row fsCreateResponse
		if status == http.StatusNotFound {
			return ""
		}
		if status != http.StatusOK || json.Unmarshal(body, &row) != nil || row.ID == "" {
			t.Fatalf("invalid preview row: %d, %s", status, body)
		}
		return row.ID
	}
	lookup := func(ref string) string { return find(t.Context(), ref) }
	var provisioned []string
	// Register before provisioning. All producers register their cleanup
	// after this one and therefore stop first, even on a test failure.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// Also discover any worker-created artifacts that the test had not
		// observed before it failed.
		for _, id := range append(provisioned, find(ctx, ref+"whiteboard"), find(ctx, memoRef)) {
			if id == "" {
				continue
			}
			status, _ := request(ctx, http.MethodDelete, "/internal/file/"+id, "", nil)
			if status != http.StatusOK && status != http.StatusNotFound {
				t.Errorf("cleanup of disposable file returned %d", status)
			}
		}
	})
	var creates atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			creates.Add(1)
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, fsURL+r.URL.RequestURI(), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := client.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(proxy.Close)
	cfg := fileServiceConfig(proxy.URL)
	cfg.Limits.IdleReleaseSeconds = 0
	a, err := app.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	transport := httptest.NewServer(a.Handler)
	t.Cleanup(transport.Close)
	base := "ws" + strings.TrimPrefix(transport.URL, "http")

	newFile := func(ref, name string, b []byte, authorized ...string) string {
		t.Helper()
		var buf bytes.Buffer
		m := multipart.NewWriter(&buf)
		for k, v := range map[string]string{"displayName": name, "storageBucketId": bucket, "externalReference": ref, "skipImageProcessing": "true"} {
			if err := m.WriteField(k, v); err != nil {
				t.Fatal(err)
			}
		}
		if len(authorized) != 0 {
			if err := m.WriteField("authorizationId", authorized[0]); err != nil {
				t.Fatal(err)
			}
		}
		f, err := m.CreateFormFile("file", name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		status, result := call(http.MethodPost, "/internal/file", m.FormDataContentType(), &buf)
		var row fsCreateResponse
		if status != http.StatusCreated || json.Unmarshal(result, &row) != nil || row.ID == "" {
			t.Fatalf("cannot provision fixture: %d %s", status, result)
		}
		provisioned = append(provisioned, row.ID)
		return row.ID
	}
	doc := ycrdt.NewDoc("empty-template")
	initial, err := ycrdt.EncodeStateAsUpdateV2(doc, nil)
	doc.Destroy()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := newFile("preview-fixture:"+board, "snapshot.bin", initial)
	img := image.NewNRGBA(image.Rect(0, 0, 512, 256))
	for i := range 512 * 256 {
		img.SetNRGBA(i%512, i/512, color.NRGBA{B: 255, A: 255})
	}
	var blank bytes.Buffer
	if err := png.Encode(&blank, img); err != nil {
		t.Fatal(err)
	}
	var full string
	card := newFile(ref+"card", "card.png", blank.Bytes())
	assetPixels := image.NewNRGBA(image.Rect(0, 0, 48, 32))
	for i := range 48 * 32 {
		assetPixels.SetNRGBA(i%48, i/48, color.NRGBA{R: 255, A: 255})
	}
	var assetPNG bytes.Buffer
	if err := png.Encode(&assetPNG, assetPixels); err != nil {
		t.Fatal(err)
	}
	asset := newFile("fixture-asset:"+board, "owned-image.png", assetPNG.Bytes(), uuid.NewString())
	register := func(instance *app.App, httpURL, id, pointer string, kind model.ContentType) {
		t.Helper()
		meta := model.Metadata{ID: model.DocumentID(id), ContentType: kind, StorageBucketID: bucket, ContentPointer: pointer, Migrated: true, OwnerRef: "test-template"}
		if err := instance.Manager.PreRegister(t.Context(), meta); err != nil {
			t.Fatal(err)
		}
		e2eCreated.Store(httpURL+"|"+id, struct{}{})
	}
	register(a, transport.URL, board, checkpoint, model.ContentTypeWhiteboard)

	c := dial(t, base, board, "whiteboard")
	c.lock()
	c.doc.Transact(func(_ *ycrdt.Transaction) {
		node := ycrdt.NewYMap(nil)
		c.doc.GetMap("elements").Set("paint", node)
		for k, v := range map[string]any{
			"id": "paint", "index": "a0", "type": "rectangle", "width": 180., "height": 70., "opacity": 100.,
			"seed": 23., "strokeWidth": 1., "strokeStyle": "solid", "fillStyle": "solid", "strokeColor": "#00ff00", "backgroundColor": "#00ff00",
		} {
			node.Set(k, v)
		}
		placed := ycrdt.NewYMap(nil)
		c.doc.GetMap("elements").Set("placed", placed)
		c.doc.GetMap("files").Set("asset", asset)
		for k, v := range map[string]any{
			"id": "placed", "index": "a1", "type": "image", "fileId": "asset", "x": 42., "y": 3.,
			"width": 96., "height": 64., "opacity": 100., "scale": ycrdt.ArrayAny{1., 1.},
			"strokeColor": "transparent", "backgroundColor": "transparent",
		} {
			placed.Set(k, v)
		}
	}, nil)
	c.unlock()

	readColor := func(id string) color.NRGBA {
		status, body := call(http.MethodGet, "/internal/file/"+id+"/content", "", nil)
		if status != http.StatusOK {
			return color.NRGBA{}
		}
		out, err := png.Decode(bytes.NewReader(body))
		if err != nil {
			t.Fatalf("decode output %s: %v", id, err)
		}
		center := out.Bounds().Max.Div(2)
		return color.NRGBAModel.Convert(out.At(center.X, center.Y)).(color.NRGBA)
	}
	if !eventuallyWithin(12*time.Second, func() bool { full = lookup(ref + "whiteboard"); return full != "" }) {
		t.Fatal("worker did not bootstrap missing image with the whiteboard still open")
	}
	for _, id := range []string{full, card} {
		want := color.NRGBA{R: 255, A: 255}
		if !eventually(func() bool { return readColor(id) == want }) {
			t.Fatalf("same existing file %s did not receive generated red preview", id)
		}
	}
	c.close()
	c = dial(t, base, board, "whiteboard")
	if !eventually(func() bool { return c.hasElement("paint") }) {
		t.Fatal("did not reopen the saved board")
	}
	c.lock()
	c.doc.GetMap("elements").Delete("paint")
	c.doc.GetMap("elements").Delete("placed")
	c.unlock()
	c.close()
	for _, id := range []string{full, card} {
		want := color.NRGBA{R: 255, G: 255, B: 255, A: 255}
		if !eventually(func() bool { return readColor(id) == want }) {
			t.Fatalf("saved clear did not blank existing file %s", id)
		}
		status, _ := call(http.MethodGet, "/rest/storage/file/"+id, "", nil)
		if status != http.StatusForbidden {
			t.Fatalf("private derivative publicly readable: %d", status)
		}
	}
	if creates.Load() != 1 || lookup(ref+"whiteboard") != full || lookup(ref+"card") != card {
		t.Fatalf("whiteboard allocated extra files or changed IDs: posts=%d", creates.Load())
	}

	memoCP := newFile("preview-fixture:"+memo, "memo-snapshot.bin", initial)
	var memoArtifact string
	register(a, transport.URL, memo, memoCP, model.ContentTypeMemo)
	c = dial(t, base, memo, "memo")
	c.lock()
	iframe := ycrdt.NewYXmlElement("iframe")
	c.doc.GetXMLFragment("default").Push(ycrdt.ArrayAny{iframe})
	iframe.SetAttribute("src", "https://preview.invalid/embedded")
	c.unlock()
	if !eventuallyWithin(12*time.Second, func() bool { memoArtifact = lookup(memoRef); return memoArtifact != "" }) {
		t.Fatal("worker did not bootstrap iframe-first Markdown with the memo still open")
	}
	readMemo := func() string {
		t.Helper()
		status, body := call(http.MethodGet, "/internal/file/"+memoArtifact+"/content", "", nil)
		if status != http.StatusOK {
			t.Fatalf("read memo artifact: %d", status)
		}
		return string(body)
	}
	// The private endpoint still returns the exact interpreter output, no
	// storage header or dependency on a completed room close.
	res := httptest.NewRecorder()
	a.MemoHandler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/internal/memos/"+memo+"/markdown", nil))
	raw := res.Body.String()
	const marker = "[//alkemio-preview-v1//]: # (Generated memo preview)"
	if res.Code != http.StatusOK || !strings.HasPrefix(raw, `<div class="iframe-wrapper"`) || !strings.Contains(raw, "<iframe ") {
		t.Fatalf("private memo endpoint did not return raw iframe Markdown: %d %q", res.Code, raw)
	}
	if stored := readMemo(); stored != marker+"\n\n"+raw {
		t.Fatalf("stored Markdown lacks the exact preview marker and body: %q", stored)
	}
	c.close()
	c = dial(t, base, memo, "memo")
	if !eventually(func() bool { return strings.Contains(c.memoText(), "preview.invalid") }) {
		t.Fatal("fresh memo session did not load saved content")
	}
	c.lock()
	frag := c.doc.GetXMLFragment("default")
	frag.Delete(0, frag.GetLength())
	c.unlock()
	c.close()
	if !eventually(func() bool {
		return strings.TrimSpace(readMemo()) == marker
	}) {
		t.Fatal("memo clear did not replace its existing artifact with blank Markdown")
	}
	if creates.Load() != 2 || lookup(memoRef) != memoArtifact {
		t.Fatalf("memo allocated extra files or changed ID: posts=%d", creates.Load())
	}
	if status, _ := call(http.MethodGet, "/rest/storage/file/"+memoArtifact, "", nil); status != http.StatusForbidden {
		t.Fatalf("private Markdown artifact publicly readable: %d", status)
	}

	transport.Close()
	a.Close()
	a2, err := app.New(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a2.Close)
	transport2 := httptest.NewServer(a2.Handler)
	t.Cleanup(transport2.Close)
	register(a2, transport2.URL, board, checkpoint, model.ContentTypeWhiteboard)
	register(a2, transport2.URL, memo, memoCP, model.ContentTypeMemo)
	base2 := "ws" + strings.TrimPrefix(transport2.URL, "http")
	for _, doc := range []struct{ id, kind string }{{board, "whiteboard"}, {memo, "memo"}} {
		dial(t, base2, doc.id, doc.kind).close()
	}
	if !eventually(func() bool { return a2.Manager.RoomCount() == 0 }) {
		t.Fatal("unchanged rooms did not release")
	}
	// The room unit test asserts zero offers for unchanged reopened documents
	// directly; a released room does not imply its worker has drained.
	if lookup(ref+"whiteboard") != full || lookup(ref+"card") != card || lookup(memoRef) != memoArtifact {
		t.Fatal("cold reads/reopen changed output rows")
	}
	t.Logf("stable preview/card/memo IDs %s/%s/%s; open-room publication allocated two missing slots; clear on close and restart preserved all IDs", full, card, memoArtifact)
}
