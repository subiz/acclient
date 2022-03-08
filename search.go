package acclient

import (
	"context"

	"github.com/subiz/header"
	"github.com/subiz/sgrpc"
)

var searchc header.SearchClient

func Search(col, accid, query, owner string, limit int64, anchor string, parts ...string) ([]*header.DocHit, string, error) {
	var owners []string
	if owner != "" && owner != "*" {
		owners = append(owners, owner)
	}

	res, err := searchc.Search(context.Background(), &header.DocSearchRequest{
		Collection:    col,
		AccountId:     accid,
		Query:         query,
		Anchor:        anchor,
		Limit:         limit,
		IncludeOwners: owners,
		DocDistinct:   true,
		IncludeParts:  parts,
	})
	if err != nil {
		return nil, "", err
	}
	return res.Hits, res.Anchor, nil
}

func SearchPart(col, accid, query, owner string, limit int64, anchor string, parts ...string) ([]*header.DocHit, string, error) {
	var owners []string
	if owner != "" && owner != "*" {
		owners = append(owners, owner)
	}

	res, err := searchc.Search(context.Background(), &header.DocSearchRequest{
		Collection:    col,
		AccountId:     accid,
		Query:         query,
		Anchor:        anchor,
		Limit:         limit,
		IncludeOwners: owners,
		DocDistinct:   false,
		IncludeParts:  parts,
	})
	if err != nil {
		return nil, "", err
	}
	return res.Hits, res.Anchor, nil
}

func Index(col, accid, doc, part, name string, owners ...string) error {
	_, err := searchc.Index(context.Background(), &header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    name,
		IsName:     true,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

func IndexName(col, accid, doc, part, content string, owners ...string) error {
	_, err := searchc.Index(context.Background(), &header.DocIndexRequest{
		Collection: col,
		AccountId:  accid,
		DocumentId: doc,
		Part:       part,
		Content:    content,
		IsName:     false,
		Owners:     owners,
	})
	if err != nil {
		return err
	}
	return nil
}

func IndexWithDay(col, accid, doc, part, content string, day int64, owners ...string) error {
	_, err := searchc.Index(context.Background(), &header.DocIndexRequest{
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
	_, err := searchc.Index(context.Background(), &header.DocIndexRequest{
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
func getSearchClient() (header.SearchClient, error) {
	if searchc != nil {
		return searchc, nil
	}

	if searchc != nil {
		return searchc, nil
	}
	if searchc == nil {
		// address: [pod name] + "." + [service name] + ":" + [pod port]
		conn, err := dialGrpc("search-0.search:12844", sgrpc.WithShardRedirect())
		if err != nil {
			return nil, err
		}
		searchc = header.NewSearchClient(conn)
	}
	return searchc, nil
}
