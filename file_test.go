package acclient

import (
	"fmt"
	"testing"

	"github.com/subiz/header"
)

func TestUploadFile(t *testing.T) {
	url, err := UploadFile("acpxkgumifuoofoosble", &header.UploadRequest{
		Name:       "thanhtest.md",
		TtlSeconds: 3600,
		Gentext:    true,
	}, []byte("Cộng hòa xã hội 6"))
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
