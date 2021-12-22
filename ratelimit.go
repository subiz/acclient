package acclient

import (
	"context"
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"github.com/subiz/sgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
)

var ratelimitc_lock = &sync.Mutex{}
var ratelimit_config map[string]*header.RateLimitEntity
var ratelimitc header.RateLimitClient

// configkey, no(timetime/window), key => usage
var ratelimit_db map[string]map[int64]map[string]int64

// configkey, timestamp(sec), key => usage
var ratelimit_newdb map[string]map[string]map[int64]int64

func init() {
	resolver.SetDefaultScheme("dns")
	ratelimit_config = make(map[string]*header.RateLimitEntity)
}

func syncRateLimit() {
	ratelimitc_lock.Lock()
	defer ratelimitc_lock.Unlock()
	client, err := getRateLimitClient()
	if err != nil {
		log.Err("subiz", err, "CANNOT SYNC RATE LIMIT")
		return
	}

	entities := make([]*header.RateLimitEntity, 0)
	for configkey, m := range ratelimit_newdb {
		entity := &header.RateLimitEntity{}
		windows := []*header.RateLimitWindow{}
		for key, timeline := range m {
			for ts, count := range timeline {
				window := &header.RateLimitWindow{Timestamp: ts, Key: key, Usage: count}
				windows = append(windows, window)
			}
		}
		entity.Configkey = configkey
		entity.Windows = windows
	}
	res, err := client.SyncRateLimit(context.Background(), &header.RateLimitSyncRequest{NewEntities: entities})
	if err != nil {
		log.Err("subiz", err, "CALL SYNC RATE ERR")
		return
	}

	ratelimit_newdb = make(map[string]map[string]map[int64]int64)
	ratelimit_db = make(map[string]map[int64]map[string]int64)
	ratelimit_config = make(map[string]*header.RateLimitEntity)
	for _, entity := range res.Entities {
		if ratelimit_db[entity.Configkey] == nil {
			ratelimit_config[entity.Configkey] = entity
			ratelimit_db[entity.Configkey] = make(map[int64]map[string]int64)
			for _, window := range entity.Windows {
				no := window.Timestamp / entity.WindowSec
				if ratelimit_db[entity.Configkey][no] == nil {
					ratelimit_db[entity.Configkey][no] = make(map[string]int64)
				}
				ratelimit_db[entity.Configkey][no][window.Key] = window.Usage
			}
			entity.Windows = nil // free mem
		}
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
		for ts, count := range m[key] {
			if no == ts/config.WindowSec {
				hot += count
			}
		}
	}
	return cold + hot
}

func LimitRate(configkey, key string) error {
	ratelimitc_lock.Lock()
	defer ratelimitc_lock.Unlock()

	ts := time.Now().Unix()
	config := ratelimit_config[configkey]
	if config != nil {
		no := ts / config.WindowSec
		lastUsage := getUsage(configkey, key, no-1)
		curUsage := getUsage(configkey, key, no)

		tspercentage := float32(config.WindowSec-(ts%config.WindowSec)) / float32(config.WindowSec)
		realUsage := int64(float32(lastUsage)*tspercentage) + curUsage
		if realUsage < config.Capacity {
			return header.E400(nil, header.E_too_many_requests)
		}
	}

	if ratelimit_newdb[configkey] == nil {
		ratelimit_newdb[configkey] = make(map[string]map[int64]int64)
	}

	if ratelimit_newdb[configkey][key] == nil {
		ratelimit_newdb[configkey][key] = make(map[int64]int64)
	}
	ratelimit_newdb[configkey][key][ts] = ratelimit_newdb[configkey][key][ts] + 1
	return nil
}

func dialGrpc(service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{}, opts...)
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBalancerName(roundrobin.Name))
	// opts = append(opts, sgrpc.WithCache())

	return grpc.Dial(service, opts...)
}

func getRateLimitClient() (header.RateLimitClient, error) {
	if ratelimitc != nil {
		return ratelimitc, nil
	}

	ratelimitc_lock.Lock()
	defer ratelimitc_lock.Unlock()
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
