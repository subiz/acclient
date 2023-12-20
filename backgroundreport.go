package acclient

import (
	"encoding/json"
	"strings"

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

type RunID string

func StartBackgroundJob(id, name string, intervalSec int) RunID {
	runid := id + "." + randomID("BJ", 28)
	b, _ := json.Marshal(&BackgroundJob{Id: id, Name: name, IntervalSec: intervalSec, LastRunId: runid})
	log.Info("2304920394BACKGROUND", "START", string(b))
	return RunID(runid)
}

// status: ok|error|warning|running|outdated
func ReportBackgroundJob(rid RunID, status string) { //
	runId := string(rid)
	jobid := strings.Split(runId, ".")[0]
	b, _ := json.Marshal(&BackgroundJob{Id: jobid, LastRunId: runId, Status: status})
	log.Info("2304920394BACKGROUND", "STATUS", string(b))
}
