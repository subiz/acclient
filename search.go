package acclient

import (
	"hash/crc32"
	"strconv"

	"github.com/subiz/header"
	"github.com/subiz/kafka"
)

func Index(col, accid, doc, part, content string, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    content,
		IsName:     false,
		Owners:     owners,
	})
}

func publishIndex(req *header.DocIndexRequest) {
	topic := "search-index-" + strconv.Itoa(int(crc32.ChecksumIEEE([]byte(req.AccountId)))%4)
	kafka.Publish(topic, req)
}

func IndexID(col, accid, doc, part, value string, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    value,
		IsId:       true,
		Owners:     owners,
	})
}

func IndexWithDay(col, accid, doc, part, content string, isName bool, isId bool, day int64, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    content,
		IsName:     isName,
		IsId:       isId,
		Day:        day,
		Owners:     owners,
	})
}
