package acclient

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/subiz/header"
	"google.golang.org/grpc"
)

type resizeTestClient struct {
	header.CdnMgrClient
	call func(context.Context, *header.ResizeImageRequest) (*header.Response, error)
}

func (c resizeTestClient) ResizeImage(ctx context.Context, r *header.ResizeImageRequest, _ ...grpc.CallOption) (*header.Response, error) {
	return c.call(ctx, r)
}

func TestResizeImage(t *testing.T) {
	original := sharedCDNClient
	defer func() { sharedCDNClient = original }()
	fid := strings.Repeat("a", 64)
	calls := 0
	response := &header.Response{AccountId: "ac01", File: &header.File{AccountId: "ac01", Id: strings.Repeat("b", 64), Type: "image/jpeg", Width: 1024, Height: 768, Url: "https://files.example/derivative"}}
	sharedCDNClient = func() header.CdnMgrClient {
		return resizeTestClient{call: func(ctx context.Context, r *header.ResizeImageRequest) (*header.Response, error) {
			calls++
			if r.AccountId != "ac01" || r.FileId != fid || r.Format != "jpeg" || r.MaxWidth != 1024 || r.MaxHeight != 1024 || r.Ctx.GetAccountId() != "ac01" || r.Ctx.GetCredential() == nil {
				t.Fatalf("wrong request: %v", r)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Error("missing operation deadline")
			}
			return response, nil
		}}
	}
	f, err := ResizeImage("ac01", fid, 1024, 1024, "jpeg")
	if err != nil || calls != 1 || f.Id != response.File.Id || f.Type != "image/jpeg" {
		t.Fatalf("unexpected result: %v %v", f, err)
	}
	f.Width = 1
	if response.File.Width != 1024 {
		t.Fatal("returned mutable RPC response")
	}
	for _, tc := range []struct {
		account, id, format string
		w, h                uint32
	}{
		{"ac01", fid, "png", 1024, 1024}, {"ac01", fid, "jpeg", 4097, 0}, {"ac01", fid, "jpeg", 0, 4097}, {"ac01", "https://example.com/a.heic", "jpeg", 1024, 1024}, {"", fid, "jpeg", 1024, 1024},
	} {
		if _, err := ResizeImage(tc.account, tc.id, tc.w, tc.h, tc.format); err == nil {
			t.Fatal("accepted invalid input", tc)
		}
	}
	if calls != 1 {
		t.Fatal("invalid input reached CDN")
	}
	response.File.Type = "image/heic"
	if _, err := ResizeImage("ac01", fid, 1024, 1024, "jpeg"); err == nil {
		t.Fatal("accepted unconverted original")
	}
	response.File.Type = "image/jpeg"
	response.File.Width = 2048
	if _, err := ResizeImage("ac01", fid, 1024, 1024, "jpeg"); err == nil {
		t.Fatal("accepted oversized derivative")
	}
	failure := errors.New("worker unavailable")
	sharedCDNClient = func() header.CdnMgrClient {
		return resizeTestClient{call: func(context.Context, *header.ResizeImageRequest) (*header.Response, error) { return nil, failure }}
	}
	if _, err := ResizeImage("ac01", fid, 1024, 1024, "jpeg"); !errors.Is(err, failure) {
		t.Fatal("lost server failure", err)
	}
}
