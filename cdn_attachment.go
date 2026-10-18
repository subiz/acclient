package acclient

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"google.golang.org/protobuf/proto"
)

// Internal traversal bookkeeping; public helpers only use header types.
type cdnMediaRef struct {
	source string
	modern bool
	image  bool
	apply  func(*header.File)
}

// PrepareAttachments explicitly imports media and returns storage-ready copies.
// Callers own authorization and must not use this to migrate history on reads.
func PrepareAttachments(accountID string, attachments []*header.Attachment) ([]*header.Attachment, error) {
	return prepareAttachments(sharedCDNClient(), accountID, attachments)
}
func prepareAttachments(client header.CdnMgrClient, account string, attachments []*header.Attachment) ([]*header.Attachment, error) {
	ctx, cancel, cred, err := cdnContext(account, nil, cdnWorkTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	out, refs, marked, err := cdnAttachmentRefs(attachments, true)
	if err != nil {
		return nil, err
	}
	cache := map[string]*header.File{}
	for _, ref := range refs {
		source := ref.source
		if ref.modern {
			fid, recognized, err := cdnReference(account, source)
			if err != nil {
				return nil, err
			}
			if !recognized {
				return nil, log.EInput(nil, "attachment", "invalid CDN file reference")
			}
			source = fid
		}
		f := cache[source]
		if f == nil {
			response, err := client.CreateUpload(ctx, &header.UploadRequest{AccountId: account, Ctx: cred, Category: "attachment", ImportUrl: source})
			if err != nil {
				return nil, err
			}
			f, err = cdnResponseFile(response, account)
			if err != nil {
				return nil, err
			}
			if f.Url == "" {
				return nil, log.EServer(nil, log.M{"reason": "CDN import returned no URL"})
			}
			if fid, ok, err := cdnReference(account, f.Url); err != nil || !ok || fid != f.Id {
				return nil, log.EServer(nil, log.M{"reason": "CDN returned an invalid canonical URL"})
			}
			f = cdnStoredFile(f)
			cache[source] = f
		}
		if ref.image && !strings.HasPrefix(f.Type, "image/") {
			return nil, log.EFileTypeMismatch(f.Type, "image/")
		}
		ref.apply(proto.Clone(f).(*header.File))
	}
	for _, a := range marked {
		a.V = "cdn"
	}
	return out, nil
}

// ResolveAttachmentURLs signs media on copies, preserving legacy attachments.
// A missing file clears its URL; file attachments also carry File.ErrorCode.
func ResolveAttachmentURLs(accountID string, attachments []*header.Attachment) ([]*header.Attachment, error) {
	return resolveAttachmentURLs(sharedCDNClient(), accountID, attachments)
}
func resolveAttachmentURLs(client header.CdnMgrClient, account string, attachments []*header.Attachment) ([]*header.Attachment, error) {
	if !cdnAccountPattern.MatchString(account) {
		return nil, log.EInput(nil, "account_id", "invalid account ID")
	}
	out, refs, _, err := cdnAttachmentRefs(attachments, false)
	if err != nil {
		return nil, err
	}
	items := []*header.CreateReadURLItem{}
	indexes := map[string]int{}
	ids := make([]string, len(refs))
	for i, ref := range refs {
		fid, ok, err := cdnReference(account, ref.source)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, log.EInput(nil, "attachment", "invalid CDN file reference")
		}
		ids[i] = fid
		if _, ok := indexes[fid]; !ok {
			indexes[fid] = len(items)
			items = append(items, &header.CreateReadURLItem{RequestId: strconv.Itoa(len(items)), Fid: fid})
		}
	}
	if len(items) == 0 {
		return out, nil
	}
	files, err := getCDNFileURLs(client, &header.BatchCreateReadURLsRequest{AccountId: account, Items: items, ExpiresInSeconds: 1800})
	if err != nil {
		return nil, err
	}
	for i, ref := range refs {
		f := proto.Clone(files[indexes[ids[i]]]).(*header.File)
		if f.Url == "" {
			f.ThumbnailUrl = ""
			f.TextUrl = ""
		}
		if ref.image && f.Url != "" && !strings.HasPrefix(f.Type, "image/") {
			return nil, log.EFileTypeMismatch(f.Type, "image/")
		}
		ref.apply(f)
	}
	return out, nil
}

// Parse references only for v=cdn attachments or URLs returned by CDN.
// The origin does not determine the attachment version or ownership.
func cdnReference(account, source string) (string, bool, error) {
	if cdnFilePattern.MatchString(source) {
		return source, true, nil
	}
	u, err := url.Parse(source)
	if err != nil || u.User != nil || u.Scheme != "https" || u.Hostname() == "" {
		return "", false, nil
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "file" || !cdnFilePattern.MatchString(parts[2]) {
		return "", false, log.EInput(nil, "attachment", "invalid CDN source path")
	}
	if parts[1] != account {
		return "", false, log.EDeny("", "", log.M{"reason": "CDN attachment account mismatch"})
	}
	return parts[2], true, nil
}
func cdnStoredFile(f *header.File) *header.File {
	out := proto.Clone(f).(*header.File)
	u, _ := url.Parse(out.Url)
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	out.Url = u.String()
	if out.ThumbnailUrl != "" {
		out.ThumbnailUrl = out.Url
	}
	out.TextUrl = ""
	out.R2Url = ""
	out.GcsUrl = ""
	out.Image_512 = ""
	out.Image_1024 = ""
	out.Ctx = nil
	return out
}

func cdnAttachmentRefs(input []*header.Attachment, prepare bool) ([]*header.Attachment, []cdnMediaRef, []*header.Attachment, error) {
	out := make([]*header.Attachment, len(input))
	for i, a := range input {
		if a != nil {
			out[i] = proto.Clone(a).(*header.Attachment)
		}
	}
	refs := []cdnMediaRef{}
	marked := []*header.Attachment{}
	addURL := func(target *string, image, modern bool) {
		if *target == "" {
			return
		}
		refs = append(refs, cdnMediaRef{source: *target, modern: modern, image: image, apply: func(f *header.File) {
			*target = f.Url
			if image && f.ThumbnailUrl != "" {
				*target = f.ThumbnailUrl
			}
		}})
	}
	addFile := func(target **header.File, modern bool) {
		f := *target
		if f == nil {
			return
		}
		source := f.Url
		if modern && f.Id != "" {
			source = f.Id
		}
		if source == "" {
			return
		}
		refs = append(refs, cdnMediaRef{source: source, modern: modern, apply: func(f *header.File) { *target = f }})
	}
	var button func(*header.MessageButton, bool)
	button = func(b *header.MessageButton, modern bool) {
		if b != nil {
			addURL(&b.ImageUrl, true, modern)
		}
	}
	quick := func(q *header.QuickReply, modern bool) {
		if q != nil {
			addURL(&q.ImageUrl, true, modern)
		}
	}
	var product func(*header.Product, bool, int) error
	product = func(p *header.Product, modern bool, depth int) error {
		if p == nil {
			return nil
		}
		if depth > 32 {
			return log.EInput(nil, "attachment", "product nesting exceeds 32")
		}
		addURL(&p.Image, true, modern)
		for i := range p.Images {
			addURL(&p.Images[i], true, modern)
		}
		for i := range p.Videos {
			addURL(&p.Videos[i], false, modern)
		}
		for _, b := range p.Buttons {
			button(b, modern)
		}
		for i := range p.Attachments {
			addFile(&p.Attachments[i], modern)
		}
		for _, o := range p.Offers {
			if o != nil {
				addURL(&o.ImageUrl, true, modern)
			}
		}
		for _, v := range p.OtherVariants {
			if err := product(v, modern, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	var walk func([]*header.Attachment, int) error
	walk = func(attachments []*header.Attachment, depth int) error {
		if depth > 32 {
			return log.EInput(nil, "attachment", "quote nesting exceeds 32")
		}
		for _, a := range attachments {
			if a == nil {
				continue
			}
			if a.V != "" && a.V != "cdn" {
				return log.EInput(nil, "attachment.v", "unsupported attachment version")
			}
			if a.Quote != nil {
				if err := walk(a.Quote.GetData().GetMessage().GetAttachments(), depth+1); err != nil {
					return err
				}
			}
			if !prepare && a.V != "cdn" {
				continue
			}
			modern := a.V == "cdn"
			if a.Type == "file" {
				source := a.Url
				if modern && a.FileId != "" {
					source = a.FileId
				} else if modern && a.File.GetId() != "" {
					source = a.File.Id
				} else if source == "" {
					source = a.File.GetUrl()
				}
				if source == "" {
					return log.EInput(nil, "attachment", "file source required")
				}
				refs = append(refs, cdnMediaRef{source: source, modern: modern, apply: func(f *header.File) {
					a.File = f
					a.FileId = f.Id
					a.Url = f.Url
					a.ThumbnailUrl = f.ThumbnailUrl
					// A per-file error may contain only identity/status. Keep stored metadata.
					if f.Type != "" {
						a.Mimetype = f.Type
						a.Size = int32(f.Size)
						a.Md5 = f.Md5
						a.Duration = f.Duration
						a.AudioWaveform = append([]float32(nil), f.AudioWaveform...)
						a.Width = f.Width
						a.Height = f.Height
						a.ThumbnailWidth = f.ThumbnailWidth
						a.ThumbnailHeight = f.ThumbnailHeight
						if f.Name != "" {
							a.Name = f.Name
						}
					}
				}})
			} else {
				addFile(&a.File, modern)
			}
			for _, e := range a.Elements {
				if e == nil {
					continue
				}
				addURL(&e.ImageUrl, true, modern)
				button(e.DefaultAction, modern)
				for _, b := range e.Buttons {
					button(b, modern)
				}
			}
			for _, b := range a.Buttons {
				button(b, modern)
			}
			for _, q := range a.QuickReplies {
				quick(q, modern)
			}
			quick(a.QuickReply, modern)
			if z := a.ZaloTemplate; z != nil {
				addURL(&z.ImageUrl, true, modern)
			}
			if z := a.ZaloContact; z != nil {
				addURL(&z.QrCodeUrl, true, modern)
				addURL(&z.Thumb, true, modern)
			}
			if err := product(a.Product, modern, 0); err != nil {
				return err
			}
			for _, it := range a.GetOrder().GetItems() {
				if it != nil {
					if err := product(it.Product, modern, 0); err != nil {
						return err
					}
				}
			}
			if f := a.Form; f != nil {
				addFile(&f.Logo, modern)
				addFile(&f.CoverImage, modern)
				for _, component := range f.Components {
					header.MapBlock(component, func(b *header.Block) {
						if b != nil {
							addFile(&b.Image, modern)
						}
					})
				}
			}
			if prepare {
				marked = append(marked, a)
			}
		}
		return nil
	}
	if err := walk(out, 0); err != nil {
		return nil, nil, nil, err
	}
	return out, refs, marked, nil
}
