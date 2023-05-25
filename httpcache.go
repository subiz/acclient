package acclient

import (
	"sync"
	"time"
)

type HttpCacheEntry struct {
	body     []byte
	mimetype string
	filename string
	created  int64
	ttlsec   int64
}

type HttpCache struct {
	lock *sync.Mutex
	data map[string]HttpCacheEntry
}

func NewHttpCache() *HttpCache {
	me := &HttpCache{lock: &sync.Mutex{}, data: map[string]HttpCacheEntry{}}
	go func() {
		for {
			time.Sleep(10 * time.Minute)
			newData := map[string]HttpCacheEntry{}
			me.lock.Lock()
			for url, entry := range me.data {
				// outdated
				if (time.Now().UnixMilli()-entry.created)/1000 > entry.ttlsec {
					continue
				}
				newData[url] = entry
			}
			me.data = newData
			me.lock.Unlock()
		}
	}()
	return me
}

func (me *HttpCache) Store(url string, body []byte, mimetype, filename string, ttlsec int64) {
	me.lock.Lock()
	me.data[url] = HttpCacheEntry{
		body:     body,
		mimetype: mimetype,
		filename: filename,
		created:  time.Now().UnixMilli(),
		ttlsec:   ttlsec,
	}
	me.lock.Unlock()
}

func (me *HttpCache) Get(url string) ([]byte, string, string, bool) {
	me.lock.Lock()
	defer me.lock.Unlock()
	entry, has := me.data[url]
	if !has {
		return nil, "", "", false
	}

	if (time.Now().UnixMilli()-entry.created)/1000 > entry.ttlsec {
		return nil, "", "", false
	}

	return entry.body, entry.mimetype, entry.filename, true
}
