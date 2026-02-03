package acclient

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/subiz/header"
	"google.golang.org/protobuf/encoding/prototext"
)

type BizbotTestCase struct {
	Bot     *header.Bot       `json:"bot"`
	Expects []*header.Message `json:"expects"`
}

func TestSearchWelcomeMsg(t *testing.T) {
	data, err := os.ReadFile("./testdata/bots.json")
	if err != nil {
		panic(err)
	}

	tcs := map[string]*BizbotTestCase{}
	if err := json.Unmarshal(data, &tcs); err != nil {
		panic(err)
	}
	for tcid, tc := range tcs {
		t.Run(tcid, func(t *testing.T) {
			out := searchWelcomeMessage(tc.Bot.Action)

			if len(out) != len(tc.Expects) {
				t.Errorf("Want len %d, Got %d", len(tc.Expects), len(out))
				return
			}

			for i := range out {
				outt, err := prototext.Marshal(out[i])
				if err != nil {
					t.Fatal(err)
				}
				expt, err := prototext.Marshal(tc.Expects[i])
				if err != nil {
					t.Fatal(err)
				}
				if string(outt) != string(expt) {
					t.Errorf("[%d] Want %s, Got %s", i, expt, outt)
				}
			}
		})
	}
}
