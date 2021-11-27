package acclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/subiz/header"
	"io/ioutil"
	"net/http"
	"strings"
)

const MAX_SIZE = 25 * 1024 * 1024 // 25MB
// const API = "https://api.subiz.com.vn"
const API = "http://api"

func UploadFileUrl(accid, url string) (*header.File, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return &header.File{}, nil
	}
	body, _ := json.Marshal(&header.FileUrlDownloadRequest{AccountId: accid, Url: url})

	resp, err := http.Post(API+"/4.0/accounts/"+accid+"/files/url/download", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, header.E500(err, header.E_subiz_call_failed)
	}

	defer resp.Body.Close()
	out, _ := ioutil.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		// try to cast to error
		e := &header.Error{}
		if jserr := json.Unmarshal(out, e); jserr == nil {
			if e.Code != "" && e.Class != 0 {
				return nil, e
			}
		}
		return nil, header.E500(err, header.E_subiz_call_failed)
	}

	file := &header.File{}
	if err = json.Unmarshal(out, file); err != nil {
		return nil, header.E500(err, header.E_invalid_json)
	}

	return file, nil
}

func UploadFile(accid, name, mimetype string, data []byte) (string, error) {
	if len(data) > MAX_SIZE {
		return "", header.E400(nil, header.E_invalid_payload_size, len(data))
	}

	presignres, err := presign(accid, &header.FileHeader{
		Name:      name,
		Size:      int64(len(data)),
		Type:      mimetype,
		AccountId: accid,
	})
	if err != nil {
		return "", err
	}

	if err := uploadFile(presignres.SignedUrl, data, mimetype); err != nil {
		return "", err
	}

	outurl, err := finishUploadFile(accid, presignres.Id)
	if err != nil {
		return "", err
	}

	return outurl, nil
}

func uploadFile(url string, data []byte, mimetype string) error {
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(data))
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	req.Header.Set("Content-Type", mimetype)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return header.E500(err, header.E_http_call_error, url)
	}
	defer resp.Body.Close()

	out, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return header.E500(nil, header.E_http_call_error, url, resp.StatusCode, string(out))
	}
	return nil
}

func finishUploadFile(accid, fileid string) (string, error) {
	fullurl := fmt.Sprintf(API+"/4.0/accounts/%s/files/%s", accid, fileid)
	resp, err := http.Post(fullurl, "application/json", nil)
	if err != nil {
		return "", header.E500(err, header.E_http_call_error)
	}

	if resp.StatusCode >= 400 {
		return "", header.E500(nil, header.E_http_call_error)
	}

	out, _ := ioutil.ReadAll(resp.Body)
	f := &header.File{}
	// config.FileUrl + body.url
	if err := json.Unmarshal(out, f); err != nil {
		return "", header.E500(nil, header.E_invalid_json, fullurl)
	}
	return "https://vcdn.subiz-cdn.com/file/" + f.Url, nil
}

func presign(accid string, f *header.FileHeader) (*header.PresignResult, error) {
	fullurl := fmt.Sprintf(API+"/4.0/accounts/%s/files", accid)

	body, _ := json.Marshal(f)
	req, err := http.NewRequest("POST", fullurl, bytes.NewBuffer(body))
	req.Header.Set("X-By", "acclient")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, header.E500(err, header.E_http_call_error, fullurl)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, header.E500(nil, header.E_http_call_error, fullurl, resp.StatusCode)
	}

	fileres := &header.PresignResult{}
	out, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(out, fileres); err != nil {
		return nil, header.E500(nil, header.E_invalid_json, fullurl)
	}
	return fileres, nil
}