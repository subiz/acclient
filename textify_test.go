package acclient

import (
	"strings"
	"testing"

	"github.com/subiz/header"
)

func TestResolveURLRejectsUnsafeSchemes(t *testing.T) {
	base := "https://example.com/docs/page.html"
	for _, raw := range []string{
		"javascript:alert(1)",
		" JaVaScRiPt:alert(1)",
		"data:text/html,<h1>x</h1>",
		"vbscript:msgbox(1)",
	} {
		if got := ResolveUrl(base, raw); got != "" {
			t.Fatalf("resolveUrl(%q) = %q, want empty", raw, got)
		}
	}

	if got := ResolveUrl(base, "../image.png"); got != "https://example.com/image.png" {
		t.Fatalf("resolveUrl(relative) = %q", got)
	}
}

func TestRemoveAllUnknownBlockSource(t *testing.T) {
	withSrc := &header.Block{
		Type:  "source",
		Attrs: map[string]string{"srcset": "https://example.com/a.png"},
	}
	removeAllUnknowBlock(withSrc)
	if withSrc.Type != "image" || withSrc.GetImage().GetUrl() != "https://example.com/a.png" {
		t.Fatalf("source with srcset converted to type=%q image=%v", withSrc.Type, withSrc.Image)
	}

	withoutSrc := &header.Block{
		Type:    "source",
		Attrs:   map[string]string{"media": "(min-width: 800px)"},
		Content: []*header.Block{{Type: "text", Text: "fallback"}},
	}
	removeAllUnknowBlock(withoutSrc)
	if withoutSrc.Type != "text" || withoutSrc.Image != nil || withoutSrc.Attrs != nil || len(withoutSrc.Content) != 0 {
		t.Fatalf("source without srcset converted to type=%q image=%v attrs=%v content=%d", withoutSrc.Type, withoutSrc.Image, withoutSrc.Attrs, len(withoutSrc.Content))
	}
}

func TestHTML2MarkdownDoesNotPromoteEscapedMarkup(t *testing.T) {
	markdown := HTML2Markdown("", "https://testdata/page.html", []byte(`<p>&lt;img src="/private.png"&gt;</p>`))
	if strings.Contains(markdown, "https://testdata/private.png") {
		t.Fatalf("escaped image markup was converted into a live image: %q", markdown)
	}
}

func TestPredictTrainingPrice(t *testing.T) {
	if got := PredictTrainingPrice(1000); got != 150_000_000 {
		t.Fatalf("PredictTrainingPrice(1000) = %d", got)
	}
	if got := PredictTrainingPrice(1500); got != 144_000_000 {
		t.Fatalf("PredictTrainingPrice(1500) = %d", got)
	}
}

func TestDownloadAsTextDocx(t *testing.T) {
	const url = "http://didong24.sbz.vn/demo.docx"
	oldApiHost := _apihost
	t.Cleanup(func() { _apihost = oldApiHost })

	_apihost = "https://api5.subiz.com.vn"
	out, err := DownloadAsText("acpxkgumifuoofoosble", url)
	if err != nil {
		t.Fatalf("DownloadAsText(%q) returned error: %v", url, err)
	}

	if !strings.Contains(out, "AZW3") {
		t.Fatalf("DownloadAsText(%q) output does not contain AZW3: %q", url, out)
	}
}
