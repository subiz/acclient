package acclient

import (
	"context"
	"hash/crc32"
	"strconv"
	"sync"

	"github.com/subiz/header"
	"github.com/subiz/kafka"
)

func Search(col, accid, query, owner string, limit int64, anchor string, filter_parts ...string) ([]*header.DocHit, string, error) {
	var owners []string
	if owner != "" && owner != "*" {
		owners = append(owners, owner)
	}

	res, err := getSearchClient().Search(context.Background(), &header.DocSearchRequest{
		Collection:    col,
		AccountId:     accid,
		Query:         query,
		Anchor:        anchor,
		Limit:         limit,
		IncludeOwners: owners,
		DocDistinct:   true,
		IncludeParts:  filter_parts,
	})
	if err != nil {
		return nil, "", err
	}
	return res.Hits, res.Anchor, nil
}

func SearchPart(col, accid, query, owner string, limit int64, anchor string, filter_parts ...string) ([]*header.DocHit, string, error) {
	var owners []string
	if owner != "" && owner != "*" {
		owners = append(owners, owner)
	}

	res, err := getSearchClient().Search(context.Background(), &header.DocSearchRequest{
		Collection:    col,
		AccountId:     accid,
		Query:         query,
		Anchor:        anchor,
		Limit:         limit,
		IncludeOwners: owners,
		DocDistinct:   false,
		IncludeParts:  filter_parts,
	})
	if err != nil {
		return nil, "", err
	}
	return res.Hits, res.Anchor, nil
}

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

func IndexPhone(col, accid, doc, part, phone string, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    phone,
		IsPhone:    true,
		Owners:     owners,
	})
}

func IndexFullname(col, accid, doc, part, fullname string, owners ...string) {
	publishIndex(&header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    fullname,
		IsName:     true,
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

func AddOwners(accid, docid string, owners ...string) {
	if len(owners) == 0 {
		return
	}
	publishIndex(&header.DocIndexRequest{
		AccountId:  accid,
		DocumentId: docid,
		Owners:     owners,
	})
}

var searchc header.SearchClient
var searchLock = &sync.Mutex{} // lock when connect to search client
func getSearchClient() header.SearchClient {
	if searchc != nil {
		return searchc
	}

	searchLock.Lock()
	if searchc != nil {
		searchLock.Unlock()
		return searchc
	}

	conn := header.DialGrpc("search-0.search:12844", header.WithShardRedirect())
	searchc = header.NewSearchClient(conn)

	searchLock.Unlock()
	return searchc
}
