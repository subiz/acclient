package acclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/log"
	"google.golang.org/protobuf/proto"
)

const cdnMaxFileSize = 25_000_000
const cdnWorkTimeout = 4 * time.Minute
const cdnReadTimeout = 30 * time.Second

var cdnAccountPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var cdnFilePattern = regexp.MustCompile(`^([a-z][a-z0-9-]{0,31}_)?[a-f0-9]{64}$`)

// Independent of waitUntilReady: file operations do not need account Cassandra.
var sharedCDNClient = sync.OnceValue(func() header.CdnMgrClient {
	address := strings.TrimSpace(os.Getenv("CDN_SERVICE"))
	if address == "" {
		address = "cdn-0.cdn:18288"
	}
	return header.NewCdnMgrClient(header.DialGrpc(address, header.WithShardRedirect()))
})
var cdnUploadHTTP = &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func cdnContext(account string, supplied *cpb.Context, timeout time.Duration) (context.Context, context.CancelFunc, *cpb.Context, error) {
	if !cdnAccountPattern.MatchString(account) {
		return nil, nil, nil, log.EInput(nil, "account_id", "invalid account ID")
	}
	var cred *cpb.Context
	if supplied != nil {
		if supplied.AccountId != "" && supplied.AccountId != account {
			return nil, nil, nil, log.EDeny("", "", log.M{"reason": "CDN account context mismatch"})
		}
		cred = proto.Clone(supplied).(*cpb.Context)
	} else {
		cred = &cpb.Context{}
	}
	cred.AccountId = account
	if cred.Credential == nil {
		cred.Credential = &cpb.Credential{Type: cpb.Type_subiz, Issuer: "acclient", Scopes: []string{"all"}}
	}
	ctx, cancel := context.WithTimeout(header.ToGrpcCtx(cred), timeout)
	return ctx, cancel, cred, nil
}
func cdnUploadRequest(req *header.UploadRequest) (*header.UploadRequest, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	out := proto.Clone(req).(*header.UploadRequest)
	if out.Category == "" {
		out.Category = "attachment"
	}
	return out, nil
}
func cdnResponseFile(out *header.Response, account string) (*header.File, error) {
	if out == nil {
		return nil, log.EServer(nil, log.M{"reason": "empty CDN response"})
	}
	if out.Error != nil {
		return nil, header.ToErr(out.Error)
	}
	f := out.File
	if f == nil || !cdnFilePattern.MatchString(f.Id) || f.AccountId != account {
		return nil, log.EServer(nil, log.M{"reason": "invalid CDN file response"})
	}
	return proto.Clone(f).(*header.File), nil
}

// UploadCDNFile sends bytes directly to R2, then publishes through CDN.
// On failure after session creation, the shared error contains upload_id.
func UploadCDNFile(accountID string, req *header.UploadRequest, data []byte) (*header.File, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	if req.AccountId != "" && req.AccountId != accountID {
		return nil, log.EInput(nil, "account_id", "request account mismatch")
	}
	req = proto.Clone(req).(*header.UploadRequest)
	req.AccountId = accountID
	return uploadCDNFile(sharedCDNClient(), cdnUploadHTTP, req, data)
}
func uploadCDNFile(client header.CdnMgrClient, transport *http.Client, req *header.UploadRequest, data []byte) (*header.File, error) {
	r, err := cdnUploadRequest(req)
	if err != nil {
		return nil, err
	}
	if r.ImportUrl != "" {
		return nil, log.EInput(nil, "import_url", "use ImportCDNFile for imports")
	}
	if len(data) > cdnMaxFileSize {
		return nil, log.EPayloadTooLarge(int64(len(data)), cdnMaxFileSize)
	}
	r.Size = int64(len(data))
	r.Sha256 = ""
	if r.ContentType == "" {
		r.ContentType = "application/octet-stream"
	}
	ctx, cancel, cred, err := cdnContext(r.AccountId, r.Ctx, cdnWorkTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	r.Ctx = cred
	out, err := client.CreateUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	if out.GetError() != nil {
		return nil, header.ToErr(out.Error)
	}
	u := out.GetUpload()
	if u == nil || u.UploadId == "" || u.UploadUrl == "" {
		return nil, log.EServer(nil, log.M{"reason": "invalid CDN upload session"})
	}
	failure := func(err error) (*header.File, error) {
		return nil, log.NewError(err, log.M{"account_id": r.AccountId, "upload_id": u.UploadId})
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, u.UploadUrl, bytes.NewReader(data))
	if err != nil {
		return failure(log.EServer(nil, log.M{"reason": "invalid CDN upload URL"}))
	}
	// Never forward credentials to R2 or follow redirects with a signed URL.
	if request.URL.Scheme != "https" && request.URL.Scheme != "http" {
		return failure(log.EServer(nil, log.M{"reason": "invalid CDN upload scheme"}))
	}
	for k, v := range u.RequiredHeaders {
		request.Header.Set(k, v)
	}
	safeTransport := *transport
	safeTransport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := safeTransport.Do(request)
	if err != nil {
		return failure(log.EServer(nil, log.M{"reason": "CDN upload transport failed"}))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return failure(log.EServer(nil, log.M{"reason": "R2 upload failed", "http_status": response.StatusCode}))
	}
	out, err = client.FinishUpload(ctx, &header.Id{AccountId: r.AccountId, Ctx: cred, Id: u.UploadId})
	if err != nil {
		return failure(err)
	}
	f, err := cdnResponseFile(out, r.AccountId)
	if err != nil {
		return failure(err)
	}
	return f, nil
}

// ImportCDNFile imports sourceURL using the supplied upload settings.
// sourceURL overrides req.ImportUrl; the input request is not modified.
func ImportCDNFile(accountID, sourceURL string, req *header.UploadRequest) (*header.File, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	if req.AccountId != "" && req.AccountId != accountID {
		return nil, log.EInput(nil, "account_id", "request account mismatch")
	}
	req = proto.Clone(req).(*header.UploadRequest)
	req.AccountId = accountID
	req.ImportUrl = sourceURL
	return importCDNFile(sharedCDNClient(), req)
}
func importCDNFile(client header.CdnMgrClient, req *header.UploadRequest) (*header.File, error) {
	r, err := cdnUploadRequest(req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(r.ImportUrl) == "" {
		return nil, log.EInput(nil, "import_url", "required")
	}
	ctx, cancel, cred, err := cdnContext(r.AccountId, r.Ctx, cdnWorkTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	r.Ctx = cred
	out, err := client.CreateUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	return cdnResponseFile(out, r.AccountId)
}
func GetCDNFile(accountID, fileID string) (*header.File, error) {
	return getCDNFile(sharedCDNClient(), accountID, fileID)
}
func getCDNFile(client header.CdnMgrClient, accountID, fileID string) (*header.File, error) {
	ctx, cancel, cred, err := cdnContext(accountID, nil, cdnReadTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if !cdnFilePattern.MatchString(fileID) {
		return nil, log.EInput(nil, "file_id", "invalid file ID")
	}
	out, err := client.GetFile(ctx, &header.Id{AccountId: accountID, Id: fileID, Ctx: cred})
	if err != nil {
		return nil, err
	}
	f, err := cdnResponseFile(out, accountID)
	if err != nil {
		return nil, err
	}
	if f.Id != fileID {
		return nil, log.EServer(nil, log.M{"reason": "CDN file identity mismatch"})
	}
	return f, nil
}
func GetCDNFileURL(accountID, fileID string) (*header.File, error) {
	files, err := GetCDNFileURLs(accountID, &header.BatchCreateReadURLsRequest{Items: []*header.CreateReadURLItem{{RequestId: "file", Fid: fileID}}})
	if err != nil {
		return nil, err
	}
	f := files[0]
	if f.Url == "" {
		code := log.E(f.ErrorCode)
		if code == "" {
			code = log.E_internal
		}
		return f, log.NewError(nil, log.M{"account_id": accountID, "file_id": fileID}, code)
	}
	return f, nil
}
func GetCDNFileURLs(accountID string, req *header.BatchCreateReadURLsRequest) ([]*header.File, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	if req.AccountId != "" && req.AccountId != accountID {
		return nil, log.EInput(nil, "account_id", "request account mismatch")
	}
	req = proto.Clone(req).(*header.BatchCreateReadURLsRequest)
	req.AccountId = accountID
	return getCDNFileURLs(sharedCDNClient(), req)
}
func getCDNFileURLs(client header.CdnMgrClient, req *header.BatchCreateReadURLsRequest) ([]*header.File, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	r := proto.Clone(req).(*header.BatchCreateReadURLsRequest)
	ctx, cancel, cred, err := cdnContext(r.AccountId, r.Ctx, cdnReadTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	r.Ctx = cred
	if r.ExpiresInSeconds < 0 || r.ExpiresInSeconds > 1800 {
		return nil, log.EInput(nil, "expires_in_seconds", "must be between 0 and 1800")
	}
	seen := map[string]bool{}
	for i, it := range r.Items {
		if it == nil || !cdnFilePattern.MatchString(it.Fid) || it.ImageMaxWidth > 4096 || it.ImageMaxHeight > 4096 {
			return nil, log.EInput(nil, "items", "invalid file or resize dimensions")
		}
		if it.RequestId == "" {
			it.RequestId = "acclient-" + strconv.Itoa(i)
		}
		if len(it.RequestId) > 128 || seen[it.RequestId] {
			return nil, log.EInput(nil, "request_id", "must be unique and at most 128 bytes")
		}
		seen[it.RequestId] = true
	}
	files := make([]*header.File, 0, len(r.Items))
	for start := 0; start < len(r.Items); start += 100 {
		batch := &header.BatchCreateReadURLsRequest{AccountId: r.AccountId, Ctx: cred, ExpiresInSeconds: r.ExpiresInSeconds, Items: r.Items[start:min(start+100, len(r.Items))]}
		out, err := client.BatchCreateReadURLs(ctx, batch)
		if err != nil {
			return nil, err
		}
		if out.GetError() != nil {
			return nil, header.ToErr(out.Error)
		}
		if len(out.GetFiles()) != len(batch.Items) {
			return nil, log.EServer(nil, log.M{"reason": "CDN batch result count mismatch"})
		}
		for i, f := range out.Files {
			if f == nil || f.Id != batch.Items[i].Fid || f.AccountId != r.AccountId {
				return nil, log.EServer(nil, log.M{"reason": "CDN batch result identity mismatch"})
			}
			files = append(files, proto.Clone(f).(*header.File))
		}
	}
	return files, nil
}
