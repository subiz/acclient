package acclient

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const cacheDir = "./.cache"

const (
	// fileurl-<md5>-<hour>.json is bucketed by clock hour: once the hour rolls
	// over, Get() computes a different path and never reads the old file again,
	// so anything older than ~1 hour is dead weight. Keep a small margin.
	fileurlTTL = 2 * time.Hour

	// shorten-<sha256> is a long-lived mapping with no expiry. Keep entries that
	// were used within this window (Get() refreshes mtime on hit), evict the rest.
	shortenTTL = 7 * 24 * time.Hour

	// Safety valve: even within TTL, never let the cache dir hold more than this
	// many files (protects the node's inodes against a sudden burst). Oldest go first.
	maxCacheFiles = 20000

	janitorInterval = 15 * time.Minute
)

var janitorOnce sync.Once

// startCacheJanitor launches a single background goroutine that prunes ./.cache.
// Safe to call multiple times; the goroutine is started only once.
func startCacheJanitor() {
	janitorOnce.Do(func() {
		go func() {
			for {
				pruneCache()
				time.Sleep(janitorInterval)
			}
		}()
	})
}

func ttlForName(name string) time.Duration {
	if strings.HasPrefix(name, "fileurl-") {
		return fileurlTTL
	}
	// shorten-* and anything else defaults to the long TTL.
	return shortenTTL
}

func pruneCache() {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return
	}
	now := time.Now()

	type survivor struct {
		path    string
		modTime time.Time
	}
	survivors := make([]survivor, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(cacheDir, e.Name())
		if now.Sub(info.ModTime()) > ttlForName(e.Name()) {
			os.Remove(path)
			continue
		}
		survivors = append(survivors, survivor{path: path, modTime: info.ModTime()})
	}

	// Safety valve: if still above the cap, drop the oldest files first.
	if len(survivors) > maxCacheFiles {
		sort.Slice(survivors, func(i, j int) bool {
			return survivors[i].modTime.Before(survivors[j].modTime)
		})
		for _, f := range survivors[:len(survivors)-maxCacheFiles] {
			os.Remove(f.path)
		}
	}
}
