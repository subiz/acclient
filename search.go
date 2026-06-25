package acclient

import (
	"strconv"

	"github.com/subiz/header"
	"github.com/subiz/kafka"
)

// activeSec should be time.Now().Unix()
func Index(col, accid, doc, part, content string, activeSec int64) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		ActiveSec:  activeSec,
		Part:       part,
		Content:    content,
		IsName:     false,
	})
}

func IndexByLocale(col, accid, doc, part, content string, locale string, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    content,
		IsName:     false,
		Owners:     owners,
		Locale:     locale,
	})
}

func publishIndex(req *header.DocIndexRequest) {
	topic := "search-index-" + strconv.Itoa(header.GetAccShard(req.GetAccountId(), 4))
	kafka.Publish("kafkaatm:9094", topic, req)
}
