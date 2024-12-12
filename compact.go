package acclient

import (
	"context"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/subiz/header"
)

var (
	compactCache2   *lru.Cache[string, int]
	uncompactCache2 *lru.Cache[int, string]
	compactLock     = &sync.Mutex{}
)

func makeSureCompact() {
	if uncompactCache2 != nil {
		return
	}
	compactLock.Lock()
	defer compactLock.Unlock()
	if uncompactCache2 != nil {
		return
	}

	compactCache2, _ = lru.New[string, int](100_000)
	uncompactCache2, _ = lru.New[int, string](100_000)
}

func CompactString2(str string) (int, error) {
	makeSureCompact()

	if str == "" {
		return 0, nil
	}

	str = strings.ToValidUTF8(str, "")
	waitUntilReady()
	number, exist := compactCache2.Get(str)
	if exist {
		return number, nil
	}

	err := session.Query(`SELECT num FROM account.compact_str2 WHERE str=?`, str).Scan(&number)
	if err == nil {
		uncompactCache2.Add(number, str)
		compactCache2.Add(str, number)
		return number, nil
	}

	numOut, err := registryClient.Compact(context.Background(), &header.String{Str: str, Version: "2"})
	if err != nil {
		return 0, err
	}
	number = int(numOut.GetNumber())
	uncompactCache2.Add(number, str)
	compactCache2.Add(str, number)
	return number, nil
}

func UncompactString2(num int) (string, error) {
	makeSureCompact()

	if num == 0 {
		return "", nil
	}
	waitUntilReady()
	str, exist := uncompactCache2.Get(num)
	if exist {
		return str, nil
	}

	err := session.Query(`SELECT str FROM account.uncompact_num2 WHERE num=?`, num).Scan(&str)
	if err == nil {
		uncompactCache2.Add(num, str)
		compactCache2.Add(str, num)
		return str, nil
	}

	strOut, err := registryClient.Uncompact(context.Background(), &header.Number{Number: int64(num), Version: "2"})
	if err != nil {
		return "", err
	}
	str = strOut.GetStr()
	uncompactCache2.Add(num, str)
	compactCache2.Add(str, num)
	return str, nil
}

func CompactStringM(strsM map[string]int) (map[string]int, error) {
	waitUntilReady()
	strsCompactM := map[string]int{}
	strsNotInCacheM := map[string]int64{}
	for str, _ := range strsM {
		if str == "" {
			strsCompactM[str] = 0
			continue
		}
		str = strings.ToValidUTF8(str, "")
		numCompacted, exist := compactCache2.Get(str)
		if exist {
			strsCompactM[str] = numCompacted
			continue
		}
		strsNotInCacheM[str] = 0
	}
	if len(strsNotInCacheM) == 0 {
		return strsCompactM, nil
	}
	strsOutM, err := registryClient.CompactM(context.Background(), &header.StrNumM{StrsM: strsNotInCacheM})
	if err != nil {
		return map[string]int{}, err
	}
	for str, num := range strsOutM.GetStrsM() {
		uncompactCache2.Add(int(num), str)
		compactCache2.Add(str, int(num))
		strsCompactM[str] = int(num)
	}
	return strsCompactM, nil
}

func UncompactStringM(numsM map[int]string) (map[int]string, error) {
	waitUntilReady()
	numsCompactedM := map[int]string{}
	for num, _ := range numsM {
		str, err := UncompactString2(num)
		if err != nil {
			return map[int]string{}, err
		}
		numsCompactedM[num] = str
	}
	return numsCompactedM, nil
}
