package acclient

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/hashicorp/golang-lru/v2/expirable"
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

const MAX_SIZE = 25 * 1024 * 1024 // 25MB
const REMOTEAPIHOST = "https://api.subiz.com.vn"
const LOCALAPIHOST = "http://api"
const BETAAPIHOST = "http://apibeta"

func init() {
	os.MkdirAll("./.cache", os.ModePerm)
	startCacheJanitor()
}

var _apihost = REMOTEAPIHOST // will switch to http://api if available
func getApiHost(accid string) string {
	if _apihost != LOCALAPIHOST {
		return _apihost
	}

	// local is available
	if header.IsStagging(accid) {
		return BETAAPIHOST
	}
	return _apihost // == LOCALAPIHOST
}

// check if local api server is available, use that instead
func loopfileapidomain() {
	for {
		if _apihost == LOCALAPIHOST {
			time.Sleep(60 * time.Second)
			continue
		}

		if resp, _ := http.Get(LOCALAPIHOST + "/ping"); resp != nil {
			out, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && strings.HasPrefix(string(out), "SUBIZAPI") {
				// local api server is available
				_apihost = LOCALAPIHOST
			}
		}
		time.Sleep(60 * time.Second)
	}
}

type FileUrlCache struct {
	cache  *expirable.LRU[string, *header.File]
	writes sync.WaitGroup
}

func (me *FileUrlCache) Set(key string, file *header.File) {
	// expirable.LRU is internally synchronized; safe to call without an outer lock.
	me.cache.Add(key, file)

	// Warm the on-disk cache off the request path — a slow disk must not block
	// the caller. The path is bucketed by clock hour, so a stale write is harmless.
	me.writes.Add(1)
	go func() {
		defer me.writes.Done()
		fileb, _ := json.Marshal(file)
		cachepath := fmt.Sprintf("./.cache/fileurl-%s-%d.json", md5sum(key), time.Now().Unix()/3600)
		os.WriteFile(cachepath, fileb, 0644)
	}()
}

func (me *FileUrlCache) Get(key string) (*header.File, bool) {
	// In-memory hit (file may be nil — a cached negative; found is still true).
	if file, found := me.cache.Get(key); found {
		return file, true
	}

	// Fall back to the on-disk cache. Deliberately lock-free: expirable.LRU has
	// its own mutex, and reading the disk under a shared lock would serialize
	// every concurrent lookup on disk I/O. Two racing readers both loading the
	// same key is harmless (idempotent Add).
	cachepath := fmt.Sprintf("./.cache/fileurl-%s-%d.json", md5sum(key), time.Now().Unix()/3600)
	cacheb, err := os.ReadFile(cachepath)
	if err != nil {
		return nil, false
	}
	file := &header.File{}
	json.Unmarshal(cacheb, file)
	me.cache.Add(key, file)
	return file, true
}

var fileurlcache = &FileUrlCache{
	cache: newFileURLLRUCache(),
}

func newFileURLLRUCache() *expirable.LRU[string, *header.File] {
	return expirable.NewLRU[string, *header.File](10240, nil, 10*time.Minute) // 10k
}

func UploadFileUrl(accid, url string) (*header.File, error) {
	return ImportCDNFile(accid, url, &header.UploadRequest{})
}

func HTMLContent2PDF(apikey, accid string, html []byte) ([]byte, error) {
	url := "https://html2pdf-457995922934.asia-southeast1.run.app/content?secret=" + apikey
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(html))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, log.ERetry(err)
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, log.ERetry(err)
	}
	return out, nil
}

// path must start with /
func HTML2PDF(apikey, path, accid, filename, content_disposition string, input any) (*header.File, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, log.EData(err, nil, log.M{"account_id": accid, "path": path, "filename": filename})
	}
	url := "https://html2pdf-457995922934.asia-southeast1.run.app/" + path
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	q := req.URL.Query()
	q.Add("secret", apikey)
	q.Add("filename", filename)
	q.Add("account-id", accid)
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid, "path": path})
	}

	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return UploadFile(accid, &header.UploadRequest{
		Name:               filename,
		Category:           "other",
		ContentDisposition: content_disposition,
	}, out)
}

func md5sum(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// Independent of waitUntilReady: file operations do not need account Cassandra.
var sharedCDNClient = sync.OnceValue(func() header.CdnMgrClient {
	return header.NewCdnMgrClient(header.DialGrpc("cdn-0.cdn:18288", header.WithShardRedirect()))
})

// Check account state without initializing the account Cassandra connection.
var sharedCDNAccountClient = sync.OnceValue(func() header.AccountMgrClient {
	return header.NewAccountMgrClient(header.DialGrpc("account-0.account:10283", header.WithShardRedirect()))
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

// cdnUploadFilename preserves the legacy HTTP API's filename normalization.
func cdnUploadFilename(name string) string {
	name = header.Ascii(name)
	var out strings.Builder
	for _, r := range name {
		if r != '-' && r != '.' && r != '_' && !unicode.IsNumber(r) && !unicode.IsLetter(r) {
			continue
		}
		out.WriteRune(r)
	}
	name = header.Norm(out.String(), 200)
	if name == "" {
		return "untitled"
	}
	return name
}

// UploadFile sends bytes directly to R2, then publishes through CDN.
// On failure after session creation, the shared error contains upload_id.
func UploadFile(accountID string, req *header.UploadRequest, data []byte) (*header.File, error) {
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	if req.AccountId != "" && req.AccountId != accountID {
		return nil, log.EInput(nil, "account_id", "request account mismatch")
	}
	req = proto.Clone(req).(*header.UploadRequest)
	req.AccountId = accountID
	req.Name = cdnUploadFilename(req.Name)
	client := sharedCDNClient()
	transport := cdnUploadHTTP

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
// Empty sources return an empty file. Successful imports use the legacy RAM/disk
// cache; as before, account state is checked only on a cache miss.
func ImportCDNFile(accountID, sourceURL string, req *header.UploadRequest) (*header.File, error) {
	sourceURL = strings.TrimSpace(sourceURL)
	if sourceURL == "" {
		return &header.File{}, nil
	}
	if strings.HasPrefix(sourceURL, "https://vcdn.subiz-cdn.com/file/https://vcdn") {
		sourceURL = strings.TrimPrefix(sourceURL, "https://vcdn.subiz-cdn.com/file/")
	}
	if req == nil {
		return nil, log.EInput(nil, "request", "required")
	}
	if req.AccountId != "" && req.AccountId != accountID {
		return nil, log.EInput(nil, "account_id", "request account mismatch")
	}
	req = proto.Clone(req).(*header.UploadRequest)
	req.AccountId = accountID
	req.ImportUrl = sourceURL

	r, err := cdnUploadRequest(req)
	if err != nil {
		return nil, err
	}
	if accountID == "" || accountID == "empty" || accountID == "undefined" {
		return nil, log.EMissingId("account_id")
	}
	ctx, cancel, cred, err := cdnContext(r.AccountId, r.Ctx, cdnWorkTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	cacheKey := cdnImportCacheKey(r)
	if r.Force == 0 {
		if f, found := fileurlcache.Get(cacheKey); found {
			if f == nil {
				return nil, nil
			}
			f = proto.Clone(f).(*header.File)
			f.R2Url = f.Url
			return f, nil
		}
	}
	r.Ctx = cred
	account, _ := sharedCDNAccountClient().GetAccount(ctx, &header.Id{AccountId: accountID, Id: accountID, Ctx: cred})
	if account.GetState() != "activated" {
		return nil, log.EAccountLocked(accountID)
	}
	if !strings.HasPrefix(sourceURL, "http://") && !strings.HasPrefix(sourceURL, "https://") && !strings.HasPrefix(sourceURL, "data:") && !cdnFilePattern.MatchString(sourceURL) {
		r.ImportUrl, err = resolveLegacyCDNSource(ctx, accountID, sourceURL)
		if err != nil {
			return nil, err
		}
	}
	out, err := sharedCDNClient().CreateUpload(ctx, r)
	if err != nil {
		return nil, err
	}
	f, err := cdnResponseFile(out, r.AccountId)
	if err != nil {
		return nil, err
	}
	if f.Url == "" {
		return nil, log.EServer(nil, log.M{"account_id": r.AccountId, "reason": "CDN returned no imported file URL"})
	}
	f.R2Url = f.Url
	fileurlcache.Set(cacheKey, proto.Clone(f).(*header.File))
	return f, nil
}

func cdnImportCacheKey(req *header.UploadRequest) string {
	r := proto.Clone(req).(*header.UploadRequest)
	r.AccountId, r.ImportUrl, r.Category = "", "", ""
	r.ImportTypeHint, r.ImportExtension = "", ""
	r.Force = 0
	// Reuse existing cache files for the old four-argument upload contract,
	// including its historical omission of extension from the cache key.
	if req.Category == "attachment" && proto.Equal(r, &header.UploadRequest{}) {
		return fmt.Sprintf("%s.%s.%s", req.AccountId, req.ImportTypeHint, req.ImportUrl)
	}
	r = proto.Clone(req).(*header.UploadRequest)
	r.Force = 0 // Force bypasses reads but refreshes the regular entry.
	wire, _ := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	return fmt.Sprintf("cdn-import:%x", sha256.Sum256(wire))
}

// Keep legacy filedb resolution in the API, which owns all of its ID/path
// aliases. Only metadata is read here; the resolved URL goes through CDN import
// with the complete UploadRequest, including validation and processing options.
func resolveLegacyCDNSource(ctx context.Context, accountID, source string) (string, error) {
	endpoint := getApiHost(accountID) + "/4.1/file/" + url.PathEscape(source) + "?account-id=" + url.QueryEscape(accountID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", log.EInternalConnect(err, log.M{"accid": accountID, "url": source})
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", log.EInternalConnect(err, log.M{"accid": accountID, "url": source})
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", log.EInternalConnect(err, log.M{"accid": accountID, "url": source})
	}
	if resp.StatusCode >= 400 {
		return "", log.EInternalConnect(nil, log.M{"accid": accountID, "url": source, "payload": header.Norm(string(body), 400)})
	}
	var file header.File
	if err = json.Unmarshal(body, &file); err != nil {
		return "", log.EData(err, nil, log.M{"_payload": string(body)})
	}
	if file.Url == "" {
		return "", log.EMissing(source, "file")
	}
	return file.Url, nil
}

func GetCDNFile(accountID, fileID string) (*header.File, error) {
	client := sharedCDNClient()
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

// ResizeImage prepares a permanent derivative of an account-owned CDN file.
// Bounds are 0 (unbounded) or at most 4096; both 0 converts format only.
// The image keeps its aspect ratio and is never cropped or enlarged.
// Format must be "jpeg" or "webp". Import external URLs with ImportCDNFile first.
// The returned File describes the derivative, including its actual size, MIME
// type and public URL. The original file is unchanged.
func ResizeImage(accountID, fileID string, maxWidth, maxHeight uint32, format string) (*header.File, error) {
	if !cdnFilePattern.MatchString(fileID) {
		return nil, log.EInput(nil, "file_id", "invalid CDN file ID", log.M{"account_id": accountID})
	}
	if maxWidth > 4096 || maxHeight > 4096 {
		return nil, log.EInput(nil, "max_width,max_height", "must be between 0 and 4096", log.M{"account_id": accountID})
	}
	if format != "jpeg" && format != "webp" {
		return nil, log.EInput(nil, "format", "must be jpeg or webp", log.M{"account_id": accountID})
	}
	ctx, cancel, cred, err := cdnContext(accountID, nil, cdnWorkTimeout)
	if err != nil {
		return nil, err
	}
	defer cancel()
	out, err := sharedCDNClient().ResizeImage(ctx, &header.ResizeImageRequest{
		AccountId: accountID, Ctx: cred, FileId: fileID,
		MaxWidth: maxWidth, MaxHeight: maxHeight, Format: format,
	})
	if err != nil {
		return nil, err
	}
	f, err := cdnResponseFile(out, accountID)
	if err != nil {
		return nil, err
	}
	if f.Url == "" || f.Type != "image/"+format || f.Width <= 0 || f.Height <= 0 ||
		(maxWidth > 0 && f.Width > int64(maxWidth)) || (maxHeight > 0 && f.Height > int64(maxHeight)) {
		return nil, log.EServer(nil, log.M{"account_id": accountID, "reason": "invalid CDN resized image response"})
	}
	return f, nil
}
