package acclient

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var metricmaplock = &sync.Mutex{}
var metricmap = make(map[string]float64)

func init() {
	go flush()
}

func flush() {
	// flush periodically in 30s
	for {
		start := time.Now()
		metricmaplock.Lock()
		metricmapcopy := make(map[string]float64)
		for k, v := range metricmap {
			metricmapcopy[k] = v
		}
		metricmap = make(map[string]float64)
		metricmaplock.Unlock()

		for k, v := range metricmapcopy {
			resp, err := http.Post("http://metric-0.metric/gauges/"+k+"/collects/"+strconv.FormatFloat(v, 'E', -1, 32), "", nil)
			if err != nil {
				fmt.Println("METRIC ERR", err.Error())
			}
			if resp.StatusCode != 200 {
				fmt.Println("METRIC ERR", resp.StatusCode)
			}
			if resp != nil {
				resp.Body.Close()
			}
		}

		if len(metricmapcopy) > 0 {
			fmt.Println("METRIC FLUSHED:", len(metricmapcopy), "in", time.Since(start))
		}
		time.Sleep(30 * time.Second)
	}
}

// min
func CollectGause(metric string, value float64) {
	metricmaplock.Lock()
	metricmap[metric] = metricmap[metric] + value
	metricmaplock.Unlock()
}
