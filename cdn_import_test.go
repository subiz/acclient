package acclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/subiz/header"
	pb "github.com/subiz/header/account"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type importTestClient struct {
	header.CdnMgrClient
	call func(context.Context, *header.UploadRequest) (*header.Response, error)
}

func (c importTestClient) CreateUpload(ctx context.Context, r *header.UploadRequest, _ ...grpc.CallOption) (*header.Response, error) {
	return c.call(ctx, r)
}

type importAccountTestClient struct {
	header.AccountMgrClient
	call func(context.Context, *header.Id) (*pb.Account, error)
}

func (c importAccountTestClient) GetAccount(ctx context.Context, r *header.Id, _ ...grpc.CallOption) (*pb.Account, error) {
	return c.call(ctx, r)
}

type importFixture struct {
	state                     string
	accountCalls, importCalls int
	accountErr, importErr     error
	request                   *header.UploadRequest
	file                      *header.File
}

func setupImportTest(t *testing.T) *importFixture {
	t.Helper()
	oldCDN, oldAccount, oldCache, oldHost := sharedCDNClient, sharedCDNAccountClient, fileurlcache, _apihost
	// Disk writes are asynchronous. Keep the directory alive until they finish
	// before restoring cwd, so no test writes leak into another test's cache.
	t.Chdir(t.TempDir())
	if err := os.Mkdir(".cache", 0700); err != nil {
		t.Fatal(err)
	}
	fileurlcache = &FileUrlCache{cache: newFileURLLRUCache()}
	f := &importFixture{state: "activated", file: &header.File{Id: strings.Repeat("a", 64), AccountId: "ac01", Url: "https://cdn.example/file", Type: "image/png"}}
	sharedCDNAccountClient = func() header.AccountMgrClient {
		return importAccountTestClient{call: func(ctx context.Context, id *header.Id) (*pb.Account, error) {
			f.accountCalls++
			if id.AccountId != "ac01" || id.Id != "ac01" || id.Ctx.GetAccountId() != "ac01" {
				t.Fatalf("wrong account lookup: %v", id)
			}
			if f.accountErr != nil {
				return nil, f.accountErr
			}
			return &pb.Account{State: &f.state}, nil
		}}
	}
	sharedCDNClient = func() header.CdnMgrClient {
		return importTestClient{call: func(ctx context.Context, r *header.UploadRequest) (*header.Response, error) {
			f.importCalls++
			f.request = proto.Clone(r).(*header.UploadRequest)
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			return &header.Response{File: f.file}, f.importErr
		}}
	}
	t.Cleanup(func() {
		fileurlcache.writes.Wait()
		sharedCDNClient, sharedCDNAccountClient, fileurlcache, _apihost = oldCDN, oldAccount, oldCache, oldHost
	})
	return f
}

func waitForImportCacheFile(t *testing.T, key string) string {
	t.Helper()
	path := filepath.Join(".cache", fmt.Sprintf("fileurl-%s-%d.json", md5sum(key), time.Now().Unix()/3600))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil && json.Valid(b) {
			return path
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cache file not written: %s", path)
	return ""
}

func TestImportCDNFileCompatibility(t *testing.T) {
	f := setupImportTest(t)
	for _, source := range []string{"", " \n\t"} {
		file, err := ImportCDNFile("", source, nil)
		if err != nil || !proto.Equal(file, &header.File{}) {
			t.Fatalf("empty source: %v %v", file, err)
		}
	}
	if f.accountCalls != 0 || f.importCalls != 0 {
		t.Fatal("empty source reached a service")
	}
	req := &header.UploadRequest{ImportExtension: "mp4", ImportTypeHint: "video/mp4", ImportUrl: "ignored"}
	original := proto.Clone(req)
	source := "https://vcdn.subiz-cdn.com/file/original"
	file, err := ImportCDNFile("ac01", " \nhttps://vcdn.subiz-cdn.com/file/"+source+" \t", req)
	if err != nil || file.R2Url != file.Url || file.Url == "" {
		t.Fatalf("import: %v %v", file, err)
	}
	if !proto.Equal(req, original) || f.request.ImportUrl != source || f.request.ImportExtension != "mp4" || f.request.ImportTypeHint != "video/mp4" || f.request.Category != "attachment" {
		t.Fatalf("lost mapping or mutated input: %v %v", req, f.request)
	}
	// Historical cache key omits extension. Cache hits precede account checks.
	key := "ac01.video/mp4." + source
	waitForImportCacheFile(t, key)
	file.Url = "caller mutation"
	f.state = "locked"
	req.ImportExtension = "aac"
	cached, err := ImportCDNFile("ac01", source, req)
	if err != nil || cached.Url != f.file.Url || f.importCalls != 1 || f.accountCalls != 1 {
		t.Fatalf("RAM cache: %v %v", cached, err)
	}
	fileurlcache.cache.Purge()
	cached, err = ImportCDNFile("ac01", source, req)
	if err != nil || cached.Url != f.file.Url || f.importCalls != 1 || f.accountCalls != 1 {
		t.Fatalf("disk cache: %v %v", cached, err)
	}
}

func TestImportCDNFileAccountAndErrors(t *testing.T) {
	f := setupImportTest(t)
	source := "https://example.com/file"
	for _, state := range []string{"locked", "", "deleted"} {
		f.state = state
		if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{}); !log.IsErr(err, string(log.E_locked_account)) {
			t.Fatalf("account %q: %v", state, err)
		}
	}
	if f.importCalls != 0 {
		t.Fatal("inactive account reached CDN")
	}
	f.state = "activated"
	f.accountErr = errors.New("account unavailable")
	if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{}); err == nil || f.importCalls != 0 {
		t.Fatal("account failure did not fail closed", err)
	}
	f.accountErr = nil
	f.importErr = errors.New("import unavailable")
	if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{}); !errors.Is(err, f.importErr) {
		t.Fatal("lost import error", err)
	}
	f.importErr = nil
	f.file.Url = ""
	if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{}); err == nil {
		t.Fatal("accepted missing URL")
	}
	f.file.Url = "https://cdn.example/file"
	if _, err := UploadFileUrl("ac01", source); err != nil || f.importCalls != 3 {
		t.Fatal("failure cached or UploadFileUrl did not delegate", err, f.importCalls)
	}
}

func TestImportCDNFileOptionsAndForce(t *testing.T) {
	f := setupImportTest(t)
	source := "https://example.com/file"
	requests := []*header.UploadRequest{
		{}, {ImportMaxSize: 123}, {ImportTypePrefix: "image/"}, {Gentext: true},
		{Category: "avatar"}, {MaxWidth: 32}, {ImportUserAgent: "different"},
		{Ctx: &cpb.Context{AccountId: "ac01", Credential: &cpb.Credential{Issuer: "agent"}}},
	}
	for _, req := range requests {
		if _, err := ImportCDNFile("ac01", source, req); err != nil {
			t.Fatal(err)
		}
	}
	if f.importCalls != len(requests) {
		t.Fatal("different settings shared a cache entry")
	}
	// Force must bypass both caches and refresh the default cache entry.
	f.file.Url = "https://cdn.example/refreshed"
	if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{Force: 1}); err != nil || f.importCalls != len(requests)+1 {
		t.Fatal("force used cache", err)
	}
	file, err := ImportCDNFile("ac01", source, &header.UploadRequest{})
	if err != nil || file.Url != f.file.Url || f.importCalls != len(requests)+1 {
		t.Fatal("force did not refresh cache", file, err)
	}
	before := f.accountCalls
	for _, req := range []*header.UploadRequest{nil, {AccountId: "other"}, {Ctx: &cpb.Context{AccountId: "other"}}} {
		if _, err := ImportCDNFile("ac01", source, req); err == nil {
			t.Fatal("cache bypassed request validation")
		}
	}
	if f.accountCalls != before {
		t.Fatal("invalid request reached account service")
	}
}

func TestImportCDNFileLegacy(t *testing.T) {
	f := setupImportTest(t)
	var requestedSource string
	status, body := http.StatusOK, `{"url":"https://legacy.example/source"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("account-id") != "ac01" {
			t.Errorf("wrong legacy lookup: %s %s", r.Method, r.URL)
		}
		requestedSource = strings.TrimPrefix(r.URL.Path, "/4.1/file/")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	_apihost = server.URL
	for _, source := range []string{"fiold", "file/fiold_ac01/name.png?x=1#part", "/file/fiother_ac01/name.png", "//vcdn.subiz-cdn.com/file/fiold_ac01/name.png"} {
		_, err := ImportCDNFile("ac01", source, &header.UploadRequest{Gentext: true, ImportExtension: "png", ImportMaxSize: 1234})
		if err != nil || requestedSource != source || f.request.ImportUrl != "https://legacy.example/source" || !f.request.Gentext || f.request.ImportMaxSize != 1234 || f.request.ImportExtension != "png" {
			t.Fatalf("legacy source %q: %q %v %v", source, requestedSource, f.request, err)
		}
	}
	before := f.importCalls
	for _, tc := range []struct {
		status int
		body   string
	}{{404, "missing"}, {200, "invalid json"}, {200, "{}"}, {200, "null"}} {
		status, body = tc.status, tc.body
		if _, err := ImportCDNFile("ac01", "fierror", &header.UploadRequest{}); err == nil || f.importCalls != before {
			t.Fatal("legacy lookup failure reached import", err)
		}
	}
	requestedSource = ""
	for _, source := range []string{strings.Repeat("a", 64), "attachment_" + strings.Repeat("b", 64), "data:image/png;base64,YQ==", "https://example.com/image", "http://example.com/image"} {
		if _, err := ImportCDNFile("ac01", source, &header.UploadRequest{}); err != nil || requestedSource != "" || f.request.ImportUrl != source {
			t.Fatal("CDN-native source used legacy resolver", source, err)
		}
	}
}
