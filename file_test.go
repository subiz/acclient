package acclient

import (
	"fmt"
	"testing"
)

func TestUploadFile(t *testing.T) {
	url, err := UploadFile("acpxkgumifuoofoosble", "thanhtest", "text/plain;charset=UTF-8", []byte("Cộng hòa xã hội") )
	if err != nil {
		t.Errorf("error %v", err)
	}

	fmt.Println("U", url)
}
