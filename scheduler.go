package acclient

import (
	"sync"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/idgen"
	"github.com/subiz/log"
)

type task struct {
	data []byte
	id   string
	sec  int64
}

// ks+accid => hour => []task
var tlock = &sync.Mutex{} // guard taskCache
var _cbm = map[string]func(data []byte){}
var taskCache = make(map[string]map[int64]map[string]task)

// BookTask registers a new task that will be execute in the future
// if the task is too old, it will be executed rightaway without
// storing in the database
func BookTask(ks, accid string, sec int64, data []byte) error {
	nowsec := time.Now().Unix()
	if sec < nowsec+5 {
		tlock.Lock()
		cb := _cbm[ks+accid]
		if cb == nil {
			tlock.Unlock()
			return header.E500(nil, header.E_database_error, "cb not registered")
		}
		tlock.Unlock()
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Info(accid, "EEEEEE", r)
				}
			}()
			cb(data)
		}()
		return nil
	}

	id := idgen.NewScheduleItemID()
	hour := sec / 3600
	// write to cache if the task going to be execute within 2 hours
	if sec < nowsec+3600*2 {
		tlock.Lock()
		if taskCache[ks+accid] == nil {
			taskCache[ks+accid] = make(map[int64]map[string]task)
		}
		if taskCache[ks+accid][hour] == nil {
			taskCache[ks+accid][hour] = make(map[string]task)
		}
		taskCache[ks+accid][hour][id] = task{data: data, id: id, sec: sec}
		tlock.Unlock()
	}

	waitUntilReady()
	err := session.Query("INSERT INTO account.task(ks,accid,hour,sec,id,data) VALUES(?,?,?,?,?,?)",
		ks, accid, hour, sec, id, data).Exec()
	if err != nil {
		return header.E500(err, header.E_database_error)
	}
	return nil
}

func loadTaskInHour(ks, accid string, hour int64) {
	var tasks []task
	for {
		var err error
		tasks, err = listTasks(ks, accid, hour)
		if err != nil {
			log.Err(accid, err)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	tlock.Lock()
	if taskCache[ks+accid] == nil {
		taskCache[ks+accid] = make(map[int64]map[string]task)
	}

	for _, t := range tasks {
		hour := t.sec / 3600
		if taskCache[ks+accid][hour] == nil {
			taskCache[ks+accid][hour] = make(map[string]task)
		}
		taskCache[ks+accid][hour][t.id] = t
	}
	tlock.Unlock()
}

// WaitTask scheduler, this function dont die, even the database is disconnected
func WaitTask(ks, accid string, f func(data []byte)) {
	// only accept one callback for each ks+accid
	tlock.Lock()
	if _cbm[ks+accid] != nil {
		tlock.Lock()
		return
	}
	_cbm[ks+accid] = f
	tlock.Unlock()

	go func() {
		hour := time.Now().Unix()/3600 - 48 // 2 days
		for h := hour; h < time.Now().Unix()/3600; h++ {
			loadTaskInHour(ks, accid, hour)
		}

		hour = time.Now().Unix() / 3600
		for {
			if hour > time.Now().Unix()/3600 { // too fast, slow down
				time.Sleep(5 * time.Second)
				continue
			}
			loadTaskInHour(ks, accid, hour)
			hour++
			time.Sleep(30 * time.Minute)
		}
	}()

	waitUntilReady()
	go func() {
		for {
			nowsec := time.Now().Unix()
			nowhour := nowsec / 3600
			tasks := []task{}
			tlock.Lock()
			m := taskCache[ks+accid]
			for hour, arr := range m {
				if hour > nowhour {
					continue
				}
				for _, t := range arr {
					if t.sec <= nowsec {
						tasks = append(tasks, t)
					}
				}
			}
			tlock.Unlock()

			for _, t := range tasks {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Info(accid, "EEEEEE", r)
						}
					}()
					f(t.data)
				}()

				for {
					err := session.Query(`DELETE FROM account.task WHERE ks=? AND accid=? AND hour=? AND id=?`, ks, accid, t.sec/3600, t.id).Exec()
					if err != nil {
						log.Err(accid, err, header.E_database_error, ks, t.sec, t.id)
						time.Sleep(2 * time.Second)
						continue
					}
					break
				}

				// delete task in cache so we wont execute it twice
				tlock.Lock()
				if taskCache[ks+accid][t.sec/3600] != nil {
					delete(taskCache[ks+accid][t.sec/3600], t.id)
					if len(taskCache[ks+accid][t.sec/3600]) == 0 {
						delete(taskCache[ks+accid], t.sec/3600)
					}
				}
				tlock.Unlock()
			}
			time.Sleep(3 * time.Second)
		}
	}()
}

func listTasks(ks, accid string, hour int64) ([]task, error) {
	waitUntilReady()
	tasks := make([]task, 0)
	iter := session.Query("SELECT id, sec, data FROM task_sec WHERE ks=? AND accid=? AND hour=?", ks, accid).Iter()
	var id string
	var sec int64
	var data []byte
	for iter.Scan(&id, &sec, &data) {
		d := make([]byte, len(data))
		copy(d, data)
		tasks = append(tasks, task{id: id, data: d, sec: sec})
	}
	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error)
	}
	return tasks, nil
}
