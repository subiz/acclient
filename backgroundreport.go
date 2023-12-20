package acclient

import (
	"encoding/json"
	"github.com/subiz/log"
)

type BackgroundJob struct {
	Id          string `json:"id,omitempty"`
	LastRunId   string `json:"last_run_id,omitempty"`
	IntervalSec int    `json:"interval_sec,omitempty"`
	Desc        string `json:"desc,omitempty"`
	Name        string `json:"name,omitempty"`
	Status      string `json:"status,omitempty"`
}

func RegistryBackgroundJob(id, name, description string, intervalSec int) {
	b, _ := json.Marshal(&BackgroundJob{Id: id, Name: name, Desc: description, IntervalSec: intervalSec})
	log.Info("2304920394BACKGROUND", "REGISTER", string(b))
}

func StartBackgroundJob(jobid string) string {
	runid := randomID("BJ", 28)
	b, _ := json.Marshal(&BackgroundJob{Id: jobid, LastRunId: runid})
	log.Info("2304920394BACKGROUND", "START", string(b))
	return runid
}

// status: ok|error|warning|running|outdated
func ReportBackgroundJob(jobid string, runId, status string) { //
	b, _ := json.Marshal(&BackgroundJob{Id: jobid, LastRunId: runId, Status: status})
	log.Info("2304920394BACKGROUND", "STATUS", string(b))
}
