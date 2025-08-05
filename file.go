package acclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/log"
	gocache "github.com/thanhpk/go-cache"
)

const MAX_SIZE = 25 * 1024 * 1024 // 25MB
const REMOTEAPIHOST = "https://api.subiz.com.vn"

var apihost = REMOTEAPIHOST // will switch to http://api if available

// check if local api server is available, use that instead
func loopfileapidomain() {
	for {
		if apihost == "http://api" {
			time.Sleep(60 * time.Second)
			continue
		}

		if resp, _ := http.Get("http://api/ping"); resp != nil {
			out, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && strings.HasPrefix(string(out), "SUBIZAPI") {
				// local api server is available
				apihost = "http://api"
			}
		}
		time.Sleep(60 * time.Second)
	}
}

var fileurlcache = gocache.New(60 * time.Minute)

func UploadFileUrl(accid, url string) (*header.File, error) {
	return UploadTypedFileUrl(accid, url, "", "")
}

func SummaryTextFile(accid, fileid string) (*header.File, error) {
	resp, err := http.Post(apihost+"/4.1/files/"+fileid+"/summary?account-id="+accid, "application/json", nil)
	if err != nil {
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.1/files/" + fileid + "/summary?account-id=" + accid})
	}

	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		// try to cast to error
		e := &log.AError{}
		if jserr := json.Unmarshal(out, e); jserr == nil {
			if e.Code != "" && e.Class != 0 {
				return nil, e
			}
		}
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.1/files/" + fileid + "/summary?account-id=" + accid})
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, nil, log.M{"_payload": string(out)})
	}
	return file, nil
}

func UploadImage(accid, url string, maxWidth, maxHeight int64) (*header.File, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return &header.File{}, nil
	}

	theurl := fmt.Sprintf("%s%dx%d.%s", accid, maxWidth, maxHeight, url)
	if value, found := fileurlcache.Get(theurl); found {
		if value == nil {
			return nil, nil
		}
		return value.(*header.File), nil
	}

	body, _ := json.Marshal(&header.FileUrlDownloadRequest{
		AccountId:  accid,
		Url:        url,
		TypePrefix: "image/",
		MaxWidth:   maxWidth,
		MaxHeight:  maxHeight,
	})

	resp, err := http.Post(apihost+"/4.0/accounts/"+accid+"/files/url/download", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		// try to cast to error
		e := &log.AError{}
		if jserr := json.Unmarshal(out, e); jserr == nil {
			if e.Code != "" && e.Class != 0 {
				return nil, e
			}
		}
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, nil, log.M{"_payload": string(out)})
	}
	fileurlcache.Set(theurl, file)
	return file, nil
}

func UploadTypedFileUrl(accid, url, extension, filetype string) (*header.File, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return &header.File{}, nil
	}

	theurl := fmt.Sprintf("%s.%s.%s", accid, filetype, url)
	if value, found := fileurlcache.Get(theurl); found {
		if value == nil {
			return nil, nil
		}
		return value.(*header.File), nil
	}

	body, _ := json.Marshal(&header.FileUrlDownloadRequest{
		AccountId: accid,
		Url:       url,
		TypeHint:  filetype,
		Extension: extension,
	})

	resp, err := http.Post(apihost+"/4.0/accounts/"+accid+"/files/url/download", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		// try to cast to error
		e := &log.AError{}
		if jserr := json.Unmarshal(out, e); jserr == nil {
			if e.Code != "" && e.Class != 0 {
				return nil, e
			}
		}
		return nil, log.EInternalConnect(err, log.M{"url": apihost + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, nil, log.M{"_payload": string(out)})
	}
	fileurlcache.Set(theurl, file)
	return file, nil
}

func UploadFile2(accid, name, category string, data []byte, cd string, ttlsec int64) (*header.File, error) {
	req, _ := http.NewRequest("POST", apihost+"/4.1/files", bytes.NewBuffer(data))
	q := req.URL.Query()
	q.Add("account-id", accid)
	q.Add("name", name)
	if category != "" {
		q.Add("category", category)
	}
	if cd != "" {
		q.Add("content_disposition", cd)
		req.Header.Set("Content-Disposition", cd)
	}
	if ttlsec > 0 {
		q.Add("ttl", strconv.Itoa(int(ttlsec)))
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-By", "acclient")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}
	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	res := &header.Response{}
	json.Unmarshal(out, res)
	if res.GetError() != nil {
		return nil, header.ToErr(res.Error)
	}
	if resp.StatusCode != 200 {
		return nil, log.ERetry(err, log.M{"account_id": accid, "_payload": out, "status_code": resp.StatusCode})
	}
	return res.GetFile(), nil
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
func HTML2PDF(apikey, path, accid, filename, content_disposition string, input interface{}) (*header.File, error) {
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
	return UploadFile2(accid, filename, "other", out, content_disposition, 0)
}
