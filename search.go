package acclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/sgrpc"
)

var searchc header.SearchClient
var searchLock = &sync.Mutex{} // lock when connect to search client

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

func Index(col, accid, doc, part, name string, owners ...string) error {
	_, err := getSearchClient().Index(context.Background(), &header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    name,
		IsName:     false,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

func IndexFullname(col, accid, doc, part, fullname string, owners ...string) error {
	_, err := getSearchClient().Index(context.Background(), &header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    fullname,
		IsName:     true,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

func IndexWithDay(col, accid, doc, part, content string, day int64, owners ...string) error {
	_, err := getSearchClient().Index(context.Background(), &header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    content,
		IsName:     false,
		Day:        day,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

func AddOwners(accid, docid string, owners ...string) error {
	if len(owners) == 0 {
		return nil
	}
	_, err := getSearchClient().Index(context.Background(), &header.DocIndexRequest{
		AccountId:  accid,
		DocumentId: docid,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

// not thread-safe
func getSearchClient() header.SearchClient {
	if searchc != nil {
		return searchc
	}

	searchLock.Lock()
	if searchc != nil {
		searchLock.Unlock()
		return searchc
	}

	for {
		conn, err := dialGrpc("search-0.search:12844", sgrpc.WithShardRedirect())
		if err != nil {
			fmt.Println("CANNOT CONNECT TO SEARCH SERVICE AT search-0.search:12844")
			fmt.Println("RETRY IN 5s")
			time.Sleep(5 * time.Second)
			continue
		}
		searchc = header.NewSearchClient(conn)
		break
	}

	searchLock.Unlock()
	return searchc
}
