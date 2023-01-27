package acclient

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/subiz/header"
)

var RFSHOST = "http://db-0:2306/"

var rfs_secret = ""

func init() {
	rfs_host := os.Getenv("RFS_SECRET")
	if rfs_host != "" {
		RFSHOST = rfs_host
	}

	rfs_secret = os.Getenv("RFS_SECRET")
}

func convertPathToRFSUrl(path string) string {
	return RFSHOST + strings.TrimPrefix(path, "/") + "?x-secret=" + rfs_secret
}

func RemoveFile(path string) error {
	url := convertPathToRFSUrl(path)
	req, _ := http.NewRequest("DELETE", url, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return header.E500(err, header.E_http_call_error, url)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return header.E500(nil, header.E_http_call_error, url, resp.StatusCode)
	}
	return nil
}

func TruncateFile(path string) error {
	return WriteFileBytes(path, []byte(""))
}

func WriteFile(path string, stream io.Reader) error {
	url := convertPathToRFSUrl(path)
	// resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(data))
	resp, err := http.Post(url, "application/octet-stream", stream)
	if err != nil {
		return header.E500(err, header.E_http_call_error, url)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return header.E500(nil, header.E_http_call_error, url, resp.StatusCode)
	}
	return nil
}

func WriteFileBytes(path string, data []byte) error {
	return WriteFile(path, bytes.NewBuffer(data))
}

func ReadFile(path string) (io.ReadCloser, error) {
	url := convertPathToRFSUrl(path)
	resp, err := http.Get(url)
	if err != nil {
		return nil, header.E500(err, header.E_http_call_error, url)
	}
	if resp.StatusCode == 404 {
		resp.Body.Close()
		return nil, os.ErrNotExist
	}

	return resp.Body, nil
}

func ReadFileBytes(path string) ([]byte, error) {
	stream, err := ReadFile(path)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	return ioutil.ReadAll(stream)
}

func WriteFilePipe(path string, predicate func(*os.File) error) error {
	tmpFile, err := ioutil.TempFile("/tmp", "rfs")
	if err != nil {
		return header.E500(err, header.E_file_system_error)
	}
	defer os.Remove(tmpFile.Name())

	if err := predicate(tmpFile); err != nil {
		tmpFile.Close()
		return err
	}
	tmpFile.Close()

	tmpFile, err = os.OpenFile(tmpFile.Name(), os.O_RDONLY, 0600)
	if err != nil {
		return header.E500(err, header.E_file_system_error)
	}
	defer tmpFile.Close()

	url := convertPathToRFSUrl(path)
	// resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(data))
	resp, err := http.Post(url, "application/octet-stream", tmpFile)
	if err != nil {
		return header.E500(err, header.E_http_call_error, url)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return header.E500(nil, header.E_http_call_error, url, resp.StatusCode)
	}
	return nil
}
