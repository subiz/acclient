package acclient

import (
	"sync"
	"time"
)

type Throttler struct {
	*sync.Mutex
	wait     time.Duration
	cache    map[string][]interface{}
	runningM map[string]bool
	handler  func(string, []interface{})
}

func NewThrottler(handler func(string, []interface{}), wait int64) *Throttler {
	me := &Throttler{
		Mutex:    &sync.Mutex{},
		wait:     time.Duration(wait) * time.Millisecond,
		runningM: make(map[string]bool),
		cache:    make(map[string][]interface{}),
		handler:  handler,
	}
	return me
}

func (me *Throttler) Push(key string, i interface{}) {
	me.Lock()
	me.cache[key] = append(me.cache[key], i)
	running := me.runningM[key]
	me.Unlock()
	if running {
		return
	}
	go me.run(key)
}

func (me *Throttler) run(key string) {
	me.Lock()
	if me.runningM[key] {
		me.Unlock()
		return
	}
	me.runningM[key] = true

	payloads := me.cache[key]
	me.cache[key] = make([]interface{}, 0)
	me.Unlock()
	if len(payloads) > 0 {
		me.handler(key, payloads)
	}
	time.Sleep(me.wait)

	me.Lock()
	delete(me.runningM, key)
	if len(me.cache[key]) > 0 {
		// there is unfinished job
		go me.run(key)
	}
	me.Unlock()
}

/*

	throttler := NewThrottler(100, true)

	go func() {
		for msgs := range throttler.Message {
			s := ""
			for _, msg := range msgs {
				s += msg.(string) + ","
			}
			fmt.Println("GOT", s)
		}
	}()

	for i := 0; i < 1000; i++ {
		throttler.Push("a", fmt.Sprintf("%d", i))
		if i%2 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(5 * time.Millisecond)
	}

throttler.Push("a", "1")
throttler.Push("a", "a")
throttler.Push("a", "b")
throttler.Push("a", "c")
time.Sleep(200 * time.Millisecond)

throttler.Push("a", "d")
throttler.Push("a", "e")
throttler.Push("a", "f")

time.Sleep(20 * time.Second)
*/
