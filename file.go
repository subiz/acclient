package acclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/subiz/header"
	"github.com/subiz/log"
)

const MAX_SIZE = 25 * 1024 * 1024 // 25MB
// const API = "https://api.subiz.com.vn"
const API = "http://api"

var httpCache = NewHttpCache()

func UploadFileUrl(accid, url string) (*header.File, error) {
	return UploadTypedFileUrl(accid, url, "", "")
}

func UploadImage(accid, url string, maxWidth, maxHeight int64) (*header.File, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return &header.File{}, nil
	}

	cached, _, _, has := httpCache.Get(fmt.Sprintf("%s%dx%d.%s", accid, maxWidth, maxHeight, url))
	if has {
		file := &header.File{}
		if err := json.Unmarshal(cached, file); err == nil {
			return file, nil
		}
	}

	body, _ := json.Marshal(&header.FileUrlDownloadRequest{
		AccountId:  accid,
		Url:        url,
		TypePrefix: "image/",
		MaxWidth:   maxWidth,
		MaxHeight:  maxHeight,
	})

	resp, err := http.Post(API+"/4.0/accounts/"+accid+"/files/url/download", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, log.EInternalConnect(err, log.M{"url": API + "/4.0/accounts/" + accid + "/files/url/download"})
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
		return nil, log.EInternalConnect(err, log.M{"url": API + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, nil, log.M{"_payload": string(out)})
	}

	httpCache.Store(fmt.Sprintf("%s%dx%d.%s", accid, maxWidth, maxHeight, url), out, "", "", 21600) // 6 hour
	return file, nil
}

func UploadTypedFileUrl(accid, url, extension, filetype string) (*header.File, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return &header.File{}, nil
	}

	cached, _, _, has := httpCache.Get(accid + "." + filetype + "." + url)
	if has {
		file := &header.File{}
		if err := json.Unmarshal(cached, file); err == nil {
			return file, nil
		}
	}

	body, _ := json.Marshal(&header.FileUrlDownloadRequest{
		AccountId: accid,
		Url:       url,
		TypeHint:  filetype,
		Extension: extension,
	})

	resp, err := http.Post(API+"/4.0/accounts/"+accid+"/files/url/download", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, log.EInternalConnect(err, log.M{"url": API + "/4.0/accounts/" + accid + "/files/url/download"})
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
		return nil, log.EInternalConnect(err, log.M{"url": API + "/4.0/accounts/" + accid + "/files/url/download"})
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, nil, log.M{"_payload": string(out)})
	}
	httpCache.Store(accid+"."+filetype+"."+url, out, "", "", 21600) // 6 hour
	return file, nil
}

func UploadFile(accid, name, category, mimetype string, data []byte, cd string, ttl int64) (string, error) {
	if len(data) > MAX_SIZE {
		return "", log.EPayloadTooLarge(int64(len(data)), int64(MAX_SIZE), log.M{"account_id": accid, "name": name})
	}

	presignres, err := presign(accid, &header.File{
		Name:               name,
		Size:               int64(len(data)),
		Type:               mimetype,
		AccountId:          accid,
		ContentDisposition: cd,
		Ttl:                ttl,
		Category:           category,
	})
	if err != nil {
		return "", err
	}

	if err := uploadFile(presignres.SignedUrl, data, mimetype, cd); err != nil {
		return "", err
	}

	f, err := finishUploadFile(accid, presignres.Id)
	if err != nil {
		return "", err
	}

	return "https://vcdn.subiz-cdn.com/file/" + f.Url, nil
}

func uploadFile(url string, data []byte, mimetype, cd string) error {
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	req.Header.Set("Content-Type", mimetype)
	if cd != "" {
		req.Header.Set("Content-Disposition", cd)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return log.EServer(err, log.M{"url": url})
	}
	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return log.EServer(err, log.M{"url": url, "body": string(out), "status_code": resp.StatusCode})
	}
	return nil
}

func finishUploadFile(accid, fileid string) (*header.File, error) {
	fullurl := fmt.Sprintf(API+"/4.0/accounts/%s/files/%s", accid, fileid)
	resp, err := http.Post(fullurl, "application/json", nil)
	if err != nil {
		return nil, log.EServer(err, log.M{"url": fullurl})
	}

	if resp.StatusCode >= 400 {
		return nil, log.EServer(err, log.M{"url": fullurl, "status_code": resp.StatusCode})
	}

	out, _ := io.ReadAll(resp.Body)
	f := &header.File{}
	// config.FileUrl + body.url
	if err := json.Unmarshal(out, f); err != nil {
		return nil, log.EServer(err, log.M{"url": fullurl})
	}
	return f, nil
}

func presign(accid string, f *header.File) (*header.PresignResult, error) {
	fullurl := fmt.Sprintf(API+"/4.0/accounts/%s/files", accid)

	body, _ := json.Marshal(f)
	req, _ := http.NewRequest("POST", fullurl, bytes.NewBuffer(body))
	req.Header.Set("X-By", "acclient")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, log.EServer(err, log.M{"url": fullurl, "account_id": accid})
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, log.EServer(err, log.M{"url": fullurl, "account_id": accid, "status_code": resp.StatusCode})
	}

	fileres := &header.PresignResult{}
	out, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(out, fileres); err != nil {
		return nil, log.EData(err, out, log.M{"url": fullurl, "account_id": accid})
	}
	return fileres, nil
}

func HTMLContent2PDF(html []byte) ([]byte, error) {
	url := "http://html2pdf:80/content"
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(html))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, log.EServer(err)
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, log.EServer(err)
	}

	return out, nil
}

// path must start with /
func HTML2PDF(path, accid, filename, content_disposition string, input interface{}) (*header.File, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, log.EData(err, nil, log.M{"account_id": accid, "path": path, "filename": filename})
	}
	url := "http://html2pdf:80" + path
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	q := req.URL.Query()
	q.Add("filename", filename)
	q.Add("account_id", accid)
	if content_disposition != "" {
		q.Add("content_disposition", content_disposition)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, log.EServer(err, log.M{"path": path})
	}

	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	file := &header.File{}
	if err := json.Unmarshal(out, file); err != nil {
		return nil, log.EData(err, out, log.M{"account_id": accid, "path": path, "filename": filename})
	}
	return file, nil
}
