package acclient

import (
	"fmt"
	nethtml "golang.org/x/net/html"
	neturl "net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/JohannesKaufmann/dom"
	mdconverter "github.com/JohannesKaufmann/html-to-markdown/v2/converter"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/base"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/commonmark"
	"github.com/subiz/header"
	"github.com/subiz/html2block"
)

var textLock = &sync.Mutex{}
var markdownConverter *mdconverter.Converter

// Precompiled regex (performance)
var (
	reMultiSpace   = regexp.MustCompile(`\s+`)
	reZeroWidth    = regexp.MustCompile("[\u200B\u200C\u200D\uFEFF]")
	reControlChars = regexp.MustCompile(`[\x00-\x1F\x7F]`)
)

var symbolReplacer = strings.NewReplacer(
	// ===== STRUCTURE =====
	"¶", "P\n",
	"§", " Section ",
	"\u00A0", " ", // non-breaking space
	"\t", " ",
	"\r", "\n",

	// ===== QUOTES =====
	"“", `"`, "”", `"`,
	"„", `"`,
	"‘", `'`, "’", `'`,
	"‚", `'`,
	"`", `'`,

	// ===== DASHES =====
	"–", "-",
	"—", "-",
	"−", "-",

	// ===== ELLIPSIS =====
	"…", "...",

	// ===== BULLETS =====
	"•", "-",
	"·", "-",
	"●", "-",
	"▪", "-",

	// ===== SYMBOLS =====
	"©", "(c)",
	"®", "(R)",
	"™", "TM",

	// ===== MATH =====
	"×", " x ",
	"÷", " / ",
	"±", " +/- ",
	"≈", " ~ ",
	"≠", " != ",
	"≤", " <= ",
	"≥", " >= ",

	// ===== CURRENCY =====
	"€", " EUR ",
	"£", " GBP ",
	"¥", " YEN ",
	"₫", " VND ",
	"$", " USD ",

	// ===== FRACTIONS =====
	"½", "1/2",
	"¼", "1/4",
	"¾", "3/4",

	// ===== ARROWS =====
	"→", " -> ",
	"←", " <- ",
	"⇒", " => ",
	"⇔", " <=> ",

	// ===== MISC =====
	"°", " degrees ",
	"º", " degrees ",
	"ª", "a",

	// ===== REMOVE =====
	"\u00AD", "", // soft hyphen
)

func normalizeWhitespace(s string) string {
	// normalize line endings
	s = strings.ReplaceAll(s, "\r\n", "\n")

	// collapse multiple newlines (max 2)
	s = collapseNewlines(s)

	// collapse spaces
	s = reMultiSpace.ReplaceAllString(s, " ")

	return s
}

func collapseNewlines(s string) string {
	lines := strings.Split(s, "\n")
	var result []string

	emptyCount := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			emptyCount++
			if emptyCount <= 1 {
				result = append(result, "")
			}
			continue
		}
		emptyCount = 0
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

func HTML2Markdown(accid, newurl string, html []byte) string {
	block := HTMLToBlock(header.CleanString(string(html)))
	normalizeBlockTextForLLM(block)
	removeAllBlockMdSkip(block)
	convertAllImages(accid, newurl, block)
	resolveAllLinks(newurl, block)
	removeAllBlockStyle(block)
	removeAllUnknowBlock(block)
	ShortenLinkAndImages(accid, block)
	markdown := convertHtmlToMarkdown(header.BlockToHTML(block))
	return markdown
}

func convertHtmlToMarkdown(input string) string {
	textLock.Lock()
	if markdownConverter == nil {
		markdownConverter = mdconverter.NewConverter(
			mdconverter.WithPlugins(
				base.NewBasePlugin(),
				commonmark.NewCommonmarkPlugin(),
			),
		)
		// Register a custom renderer for <h1> tags.
		tags := []string{"h1", "h2", "h3", "h4", "h5", "h6"}
		for _, tag := range tags {
			markdownConverter.Register.RendererFor(tag, mdconverter.TagTypeBlock, renderCustomHeadingWithLink, mdconverter.PriorityStandard-1)
		}
	}
	conv := markdownConverter
	textLock.Unlock()
	markdown, _ := conv.ConvertString(input)
	return header.CleanString(markdown)
}

func convertAllImages(accid, baseurl string, block *header.Block) {
	if block == nil {
		return
	}

	if block.Type == "image" && block.Image != nil {
		url := resolveUrl(baseurl, block.Image.Url)
		if strings.HasPrefix(baseurl, "https://testdata/") || strings.HasPrefix(baseurl, "http://testdata/") {
			block.Image.Url = url
		} else {
			file, err := UploadImage(accid, url, 1024, 1024)
			// fmt.Println("UPLOAD DONE", time.Since(start), file, err)
			if err != nil {
				fmt.Println("EEEEEEEEE", url, err)
			}
			// force convert, even error, take empty url instead of original
			url = strings.ReplaceAll(file.GetUrl(), "//vcdn.subiz-cdn.com/", "//cdn.subiz.net/")
			block.Image.Url = url
			block.Image.Width = file.GetWidth()
			block.Image.Height = file.GetHeight()
		}
	}

	for _, content := range block.Content {
		convertAllImages(accid, baseurl, content)
	}
}

func ShortenLinkAndImages(accid string, block *header.Block) {
	if block == nil {
		return
	}

	if block.Type == "image" && block.Image != nil {
		url := block.Image.Url
		if !strings.HasPrefix(url, "https://testdata/") && !strings.HasPrefix(url, "http://testdata/") {
			if newlink, err := ShortenLink(accid, block.GetImage().GetUrl()); err == nil {
				block.Image.Url = newlink
			}
		}
	}

	if block.Type == "link" {
		href := UnwrapGoogleLink(block.Href)
		if newlink, err := ShortenLink(accid, href); err == nil {
			block.Href = newlink
		}
	}

	for _, content := range block.Content {
		ShortenLinkAndImages(accid, content)
	}
}

func UnshortenLinkAndImages(accid string, block *header.Block) {
	if block == nil {
		return
	}

	if block.Type == "image" && block.Image != nil {
		url := block.Image.Url
		if !strings.HasPrefix(url, "https://testdata/") && !strings.HasPrefix(url, "http://testdata/") {
			link, _ := LookupLink(block.GetImage().GetUrl())
			if link.GetUrl() != "" {
				block.Image.Url = link.GetUrl()
			}
		}
	}

	if block.Type == "link" {
		changeText := false
		if block.Text == block.Href {
			changeText = true
		}

		href := UnwrapGoogleLink(block.Href)

		link, _ := LookupLink(href)
		if link.GetUrl() != "" {
			block.Href = link.GetUrl()
			if changeText {
				block.Text = block.Href
			}
		}
	}

	for _, content := range block.Content {
		UnshortenLinkAndImages(accid, content)
	}
}

var AllowBlockTypeM = map[string]string{
	"figure":          "paragraph",
	"div":             "div",
	"image":           "image",
	"link":            "link",
	"paragraph":       "paragraph",
	"source":          "image",
	"picture":         "paragraph", // TODO handle this
	"heading":         "heading",
	"text":            "text",
	"section":         "section",
	"bullet_list":     "bullet_list",
	"list_item":       "list_item",
	"span":            "span", // ???
	"pre":             "pre",
	"table":           "table",
	"table_row":       "table_row",
	"table_cell":      "table_cell",
	"ordered_list":    "ordered_list",
	"blockquote":      "blockquote",
	"mrkdwn":          "pre",
	"emoji":           "emoji",
	"horizontal_rule": "horizontal_rule",
	"dynamic-field":   "dynamic-field",
	"mention":         "mention",
	"noscript":        "-",
}

func removeAllUnknowBlock(block *header.Block) {
	if block == nil {
		return
	}

	block.Style = nil
	block.Class = ""
	block.Type = strings.ToLower(block.Type)
	totype := AllowBlockTypeM[block.Type]
	if totype == "" {
		totype = "paragraph"
	}

	if totype == "-" {
		block.Type = "text"
		block.Id = ""
		block.Content = nil
		block.Attrs = nil
		return
	}

	if block.Type == "source" {
		attrs := block.Attrs
		if attrs == nil {
			attrs = map[string]string{}
		}
		src := attrs["srcset"]
		if src == "" {
			src = attrs["data-placeholder"]
		}
		if src != "" {
			block.Image = &header.File{
				Url: src,
			}
		} else {
			block.Type = "text"
			block.Id = ""
			block.Content = nil
			block.Attrs = nil
			return
		}
	}
	block.Type = totype
	block.Attrs = nil
	for _, content := range block.Content {
		removeAllUnknowBlock(content)
	}
}

func removeAllBlockStyle(block *header.Block) {
	if block == nil {
		return
	}
	block.Style = nil
	block.Class = ""
	for _, content := range block.Content {
		removeAllBlockStyle(content)
	}
}

func removeAllBlockMdSkip(block *header.Block) {
	if block == nil {
		return
	}
	if strings.Contains(block.Class, "md-skip") {
		block.Text = ""
		block.Content = nil
		return
	}
	for _, content := range block.Content {
		removeAllBlockMdSkip(content)
	}
}

func resolveAllLinks(baseurl string, block *header.Block) {
	if block == nil {
		return
	}

	if block.Type == "link" {
		block.Href = resolveUrl(baseurl, block.Href)
	}

	for _, content := range block.Content {
		resolveAllLinks(baseurl, content)
	}
}

// Resolve resolves a relative URL against a base URL
func resolveUrl(baseURL string, relative string) string {
	relative = strings.TrimSpace(relative)
	if relative == "" {
		return ""
	}

	rel, err := neturl.Parse(relative)
	if err != nil {
		return relative
	}
	if isUnsafeURLScheme(rel.Scheme) {
		return ""
	}

	base, err := neturl.Parse(baseURL)
	if err != nil {
		return relative
	}

	resolved := base.ResolveReference(rel)
	if isUnsafeURLScheme(resolved.Scheme) {
		return ""
	}
	return resolved.String()
}

func isUnsafeURLScheme(scheme string) bool {
	return strings.EqualFold(scheme, "javascript") ||
		strings.EqualFold(scheme, "data") ||
		strings.EqualFold(scheme, "vbscript")
}

func UnwrapGoogleLink(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}

	if !isGoogleHost(u.Hostname()) {
		return raw
	}
	path := u.Path

	// AMP formats
	// /amp/s/target.com/x
	if after, ok := strings.CutPrefix(path, "/amp/s/"); ok {
		return "https://" + after
	}

	// target-com.cdn.ampproject.org/c/s/target.com/x
	if strings.Contains(u.Host, ".ampproject.org") {
		if _, after, ok := strings.Cut(path, "/c/s/"); ok {
			return "https://" + after
		}
		if _, after, ok := strings.Cut(path, "/c/"); ok {
			return "http://" + after
		}
	}

	if !isGoogleRedirectPath(path) {
		return raw
	}

	q := u.Query()

	// /url?q= or /redirect?url=
	if v := q.Get("q"); v != "" {
		return v
	}
	if v := q.Get("url"); v != "" {
		return v
	}
	if v := q.Get("link"); v != "" {
		return v
	}

	return raw
}

func isGoogleHost(h string) bool {
	return h == "google.com" ||
		strings.HasSuffix(h, ".google.com") ||
		strings.HasSuffix(h, ".ampproject.org") ||
		strings.HasSuffix(h, ".page.link") ||
		strings.HasSuffix(h, ".doubleclick.net") ||
		strings.HasSuffix(h, ".youtube.com")
}

func isGoogleRedirectPath(p string) bool {
	switch {
	case p == "/url":
		return true
	case p == "/redirect":
		return true
	case strings.HasPrefix(p, "/amp/s/"):
		return true
	}
	return false
}

func HTMLToBlock(htmlStr string) *header.Block {
	htmlStr = strings.TrimSpace(htmlStr)
	if htmlStr == "" {
		return &header.Block{}
	}
	return html2block.HTML2Block(htmlStr)
}

const MARKDOWNLINKMARKER = "ALINK3144"

func renderCustomHeadingWithLink(ctx mdconverter.Context, w mdconverter.Writer, node *nethtml.Node) mdconverter.RenderStatus {
	// Get the id attribute
	id, _ := dom.GetAttribute(node, "id")

	// To get the content of the node, we can render the child nodes to a
	// temporary buffer and then get the string.
	var buf strings.Builder
	ctx.RenderChildNodes(ctx, &buf, node)
	text := strings.TrimSpace(buf.String())

	// Determine the heading level
	level := 0
	switch dom.NodeName(node) {
	case "h1":
		level = 1
	case "h2":
		level = 2
	case "h3":
		level = 3
	case "h4":
		level = 4
	case "h5":
		level = 5
	case "h6":
		level = 6
	}

	headingPrefix := strings.Repeat("#", level)
	if id != "" {
		id = strings.Split(id, ")")[0]
		id = header.Norm(id, 500)
		// Write the custom heading format with a link
		w.WriteString(fmt.Sprintf("%s %s[%s](#%s)\n", headingPrefix, text, MARKDOWNLINKMARKER, id))
	} else {
		// Fallback for h1 without id
		w.WriteString(fmt.Sprintf("%s %s\n", headingPrefix, text))
	}

	// Return RenderSuccess to indicate that we have handled the node.
	return mdconverter.RenderSuccess
}

func NormalizeForLLM(s string) string {
	// 1. Decode ALL HTML entities
	s = nethtml.UnescapeString(s)

	// 2. Remove invisible / zero-width chars
	s = reZeroWidth.ReplaceAllString(s, "")

	// 3. Normalize common problematic unicode
	s = symbolReplacer.Replace(s)
	// 4. Normalize whitespace early
	s = normalizeWhitespace(s)

	// 5. Remove control characters
	s = reControlChars.ReplaceAllString(s, "")

	// 6. Final cleanup
	s = strings.TrimSpace(s)
	return s
}

func normalizeBlockTextForLLM(block *header.Block) {
	if block == nil {
		return
	}
	if block.Text != "" {
		block.Text = NormalizeForLLM(block.Text)
	}
	if block.Title != "" {
		block.Title = NormalizeForLLM(block.Title)
	}
	if block.AltText != "" {
		block.AltText = NormalizeForLLM(block.AltText)
	}
	for _, content := range block.Content {
		normalizeBlockTextForLLM(content)
	}
}

type TrainingPricePoint struct {
	Size  int
	Price *float64 // dùng pointer để biểu diễn giá có thể bị thiếu
}

//go:fix inline
func floatPtr(v float64) *float64 {
	return new(v)
}

var trainingPriceTable = []TrainingPricePoint{
	{0, new(float64(0))},
	{1000, new(float64(150))},
	{2000, new(float64(138))},
	{4000, new(float64(126))},
	{5000, new(float64(217))},
	{10000, new(float64(322))},
	{13000, new(float64(800))},
	{20000, new(float64(1000))},
	{50000, new(float64(3000))},
	{100000, new(float64(5000))},
}

// size -> fpvvnd
func PredictTrainingPrice(size int) int64 {
	data := append([]TrainingPricePoint(nil), trainingPriceTable...)
	// sort theo size (phòng trường hợp input chưa sort)
	sort.Slice(data, func(i, j int) bool {
		return data[i].Size < data[j].Size
	})

	// nếu trùng size và có price → trả luôn
	for _, p := range data {
		if p.Size == size && p.Price != nil {
			return int64(*p.Price * 1000000)
		}
	}

	var left *TrainingPricePoint
	var right *TrainingPricePoint

	// tìm 2 điểm gần nhất có price
	for i := range data {
		if data[i].Price == nil {
			continue
		}

		if data[i].Size <= size {
			left = &data[i]
		}

		if data[i].Size > size {
			right = &data[i]
			break
		}
	}

	// nếu không tìm được 1 trong 2 phía
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return int64(*right.Price * 1000000)
	}
	if right == nil {
		return int64(*left.Price * 1000000)
	}

	// nội suy tuyến tính
	x1, y1 := float64(left.Size), *left.Price
	x2, y2 := float64(right.Size), *right.Price

	return int64((y1 + (float64(size)-x1)*(y2-y1)/(x2-x1)) * 1000000)
}
