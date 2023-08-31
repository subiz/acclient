package acclient

import (
	"context"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/subiz/header"
)

var (
	registryClient header.NumberRegistryClient

	compactCache, _   = lru.New[string, int](10_000)
	uncompactCache, _ = lru.New[int, string](10_000)
)

func CompactString(str string) (int, error) {
	waitUntilReady()
	number, exist := compactCache.Get(str)
	if exist {
		return number, nil
	}

	err := session.Query(`SELECT num FROM account.compact_str WHERE str=?`, str).Scan(&number)
	if err == nil {
		uncompactCache.Add(number, str)
		compactCache.Add(str, number)
		return number, nil
	}

	numOut, err := registryClient.Compact(context.Background(), &header.String{Str: str})
	if err != nil {
		return 0, err
	}
	number = int(numOut.GetNumber())
	uncompactCache.Add(number, str)
	compactCache.Add(str, number)
	return number, nil
}

func UncompactString(num int) (string, error) {
	waitUntilReady()

	str, exist := uncompactCache.Get(num)
	if exist {
		return str, nil
	}

	err := session.Query(`SELECT str FROM account.uncompact_num WHERE num=?`, num).Scan(&str)
	if err == nil {
		uncompactCache.Add(num, str)
		compactCache.Add(str, num)
		return str, nil
	}

	strOut, err := registryClient.Uncompact(context.Background(), &header.Number{Number: int64(num)})
	if err != nil {
		return "", err
	}
	str = strOut.GetStr()
	uncompactCache.Add(num, str)
	compactCache.Add(str, num)
	return str, nil
}
