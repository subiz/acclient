package acclient

import (
	"context"
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"github.com/subiz/sgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ratelimitc_lock = &sync.Mutex{}
var ratelimitc header.RateLimitClient
var ratelimit_config map[string]*header.RateLimitEntity

// configkey, no(timetime/window), key => usage
var ratelimit_db map[string]map[int64]map[string]int64

// configkey, timestamp(sec), key => usage
var ratelimit_newdb map[string]map[string]map[int64]int64

// configkey, timestamp(sec), key => usage
var ratelimit_tmpdb map[string]map[string]map[int64]int64

var _initRateLimit = false

func waitInitRatelimit() {
	if _initRateLimit {
		return
	}
	ratelimitc_lock.Lock()
	if _initRateLimit {
		ratelimitc_lock.Unlock()
		return
	}
	_initRateLimit = true
	ratelimit_config = make(map[string]*header.RateLimitEntity)
	ratelimit_db = make(map[string]map[int64]map[string]int64)
	ratelimit_newdb = make(map[string]map[string]map[int64]int64)
	ratelimit_tmpdb = make(map[string]map[string]map[int64]int64)
	ratelimitc_lock.Unlock()

	go func() {
		for {
			syncRateLimit()
			time.Sleep(30 * time.Second)
		}
	}()
}

func syncRateLimit() {
	ratelimitc_lock.Lock()

	// clean first
	// maybe mem leak, since go map wont shrink after deletion
	// see https://github.com/golang/go/issues/20135
	for configkey, m := range ratelimit_db {
		config := ratelimit_config[configkey] // alway has value
		no := time.Now().Unix() / config.WindowSec
		for nots := range m {
			if nots < no-2 {
				delete(m, nots)
			}
		}
	}
	// --- done cleaning

	entities := make([]*header.RateLimitEntity, 0)
	existedKey := map[string]bool{}
	for configkey, m := range ratelimit_newdb {
		config := ratelimit_config[configkey] // always have value
		windows := []*header.RateLimitWindow{}
		for key, timeline := range m {
			for no, count := range timeline {
				existedKey[key] = true
				window := &header.RateLimitWindow{Timestamp: no * config.WindowSec, Key: key, Usage: count}
				windows = append(windows, window)
			}
		}
		entities = append(entities, &header.RateLimitEntity{Configkey: configkey, Windows: windows})
	}

	for configkey, m := range ratelimit_tmpdb {
		windows := []*header.RateLimitWindow{}
		for key, timeline := range m {
			for ts, count := range timeline {
				existedKey[key] = true
				window := &header.RateLimitWindow{Timestamp: ts, Key: key, Usage: count}
				windows = append(windows, window)
			}
		}
		entities = append(entities, &header.RateLimitEntity{Configkey: configkey, Windows: windows})
	}

	for configkey, m := range ratelimit_db {
		config := ratelimit_config[configkey] // always have value
		for no, timeline := range m {
			for key := range timeline {
				if existedKey[key] {
					continue
				}
				existedKey[key] = true

				var foundEntity *header.RateLimitEntity
				for _, entity := range entities {
					if entity.Configkey == configkey {
						foundEntity = entity
						break
					}
				}

				if foundEntity == nil {
					foundEntity = &header.RateLimitEntity{Configkey: configkey}
					entities = append(entities, foundEntity)
				}

				var foundWindow *header.RateLimitWindow
				for _, window := range foundEntity.Windows {
					if window.GetKey() == key {
						foundWindow = window
						break
					}
				}

				if foundWindow == nil {
					foundEntity.Windows = append(foundEntity.Windows, &header.RateLimitWindow{Key: key, Timestamp: no * config.WindowSec})
				}
			}
		}
	}

	// reset
	ratelimit_newdb = make(map[string]map[string]map[int64]int64)
	ratelimit_tmpdb = make(map[string]map[string]map[int64]int64)
	ratelimitc_lock.Unlock()

	client, err := getRateLimitClient()
	if err != nil {
		log.Err("subiz", err, "CANNOT SYNC RATE LIMIT")
		return
	}

	res, err := client.SyncRateLimit(context.Background(), &header.RateLimitSyncRequest{NewEntities: entities})
	if err != nil {
		log.Err("subiz", err, "CALL SYNC RATE ERR")
		return
	}
	ratelimitc_lock.Lock()
	defer ratelimitc_lock.Unlock()
	// ratelimit_config = make(map[string]*header.RateLimitEntity)
	for _, entity := range res.Entities {
		if ratelimit_db[entity.Configkey] == nil {
			ratelimit_db[entity.Configkey] = make(map[int64]map[string]int64)
		}
		ratelimit_config[entity.Configkey] = entity
		for _, window := range entity.Windows {
			no := window.Timestamp / entity.WindowSec
			if ratelimit_db[entity.Configkey][no] == nil {
				ratelimit_db[entity.Configkey][no] = make(map[string]int64)
			}
			ratelimit_db[entity.Configkey][no][window.Key] = window.Usage
		}
		ratelimit_config[entity.Configkey].Windows = nil // free mem
	}
}

func getUsage(configkey, key string, no int64) int64 {
	config := ratelimit_config[configkey]
	if config == nil {
		return 0
	}
	cold := int64(0)
	if m, has := ratelimit_db[configkey]; has {
		if timeline, hastl := m[no]; hastl {
			cold = timeline[key]
		}
	}

	hot := int64(0)
	if m, has := ratelimit_newdb[configkey]; has {
		if timeline, hastl := m[key]; hastl {
			hot = timeline[no]
		}
	}
	return cold + hot
}

func LimitRate(configkey, key string) error {
	waitInitRatelimit()

	ratelimitc_lock.Lock()
	defer func() {
		recover()
		ratelimitc_lock.Unlock()
	}()

	ts := time.Now().Unix()
	config := ratelimit_config[configkey]
	if config == nil {
		if ratelimit_tmpdb[configkey] == nil {
			ratelimit_tmpdb[configkey] = make(map[string]map[int64]int64)
		}

		if ratelimit_tmpdb[configkey][key] == nil {
			ratelimit_tmpdb[configkey][key] = make(map[int64]int64)
		}
		ratelimit_tmpdb[configkey][key][ts] = ratelimit_tmpdb[configkey][key][ts] + 1
		return nil
	}

	no := ts / config.WindowSec
	lastUsage := getUsage(configkey, key, no-1)
	curUsage := getUsage(configkey, key, no)

	tspercentage := float32(config.WindowSec-(ts%config.WindowSec)) / float32(config.WindowSec)
	realUsage := int64(float32(lastUsage)*tspercentage) + curUsage

	if realUsage > config.Capacity {
		return log.ELimitExceeded(realUsage, config.Capacity, log.M{"key": key})
	}

	if ratelimit_newdb[configkey] == nil {
		ratelimit_newdb[configkey] = make(map[string]map[int64]int64)
	}

	if ratelimit_newdb[configkey][key] == nil {
		ratelimit_newdb[configkey][key] = make(map[int64]int64)
	}
	ratelimit_newdb[configkey][key][no] = ratelimit_newdb[configkey][key][no] + 1

	return nil
}

func dialGrpc(service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{}, opts...)
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// Enabling WithBlock tells the client to not give up trying to find a server
	// opts = append(opts, sgrpc.WithCache())

	return grpc.Dial(service, opts...)
}

// not thread-safe
func getRateLimitClient() (header.RateLimitClient, error) {
	if ratelimitc != nil {
		return ratelimitc, nil
	}

	if ratelimitc != nil {
		return ratelimitc, nil
	}
	if ratelimitc == nil {
		// address: [pod name] + "." + [service name] + ":" + [pod port]
		conn, err := dialGrpc("ratelimit-0.ratelimit:12842", sgrpc.WithShardRedirect())
		if err != nil {
			return nil, err
		}
		ratelimitc = header.NewRateLimitClient(conn)
	}
	return ratelimitc, nil
}
