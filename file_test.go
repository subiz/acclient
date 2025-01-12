package acclient

import (
	"fmt"
	"testing"
)

func TestRandomId(t *testing.T) {
	fmt.Println("RANDOM", randomID("", 28))
}

func TestUploadFile(t *testing.T) {
	url, err := UploadFile2("acpxkgumifuoofoosble", "thanhtest", "text/plain;charset=UTF-8", []byte("Cộng hòa xã hội"), "")
	if err != nil {
		t.Errorf("error %v", err)
	}

	fmt.Println("U", url)
}

func TestUploadFileUrl(t *testing.T) {
	url, err := UploadFileUrl("acpxkgumifuoofoosble", "https://avatars.githubusercontent.com/u/1810201?s=100&v=4")
	if err != nil {
		t.Errorf("error %v", err)
	}

	fmt.Println("U", url)
}
