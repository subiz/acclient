package acclient

import (
	"encoding/json"
	"strings"

	"github.com/subiz/log"
)

type HealthCheckSample struct {
	CheckId string `json:"check_id,omitempty"`
	Status  string `json:"status,omitempty"` // ok, failed
	Created int64  `json:"status,omitempty"` // ms
	Meta    string `json:"meta,omitempty"`   // json encoded
}

type BackgroundJob struct {
	Id          string `json:"id,omitempty"`
	LastRunId   string `json:"last_run_id,omitempty"`
	IntervalSec int    `json:"interval_sec,omitempty"`
	Name        string `json:"name,omitempty"`
	Status      string `json:"status,omitempty"`
}

type RunID string

// status: ok|error|warning|running|outdated
// ReportHealthCheck("db-0.cpu", "ok", "cpu=64%")
func ReportHealthCheck(checkid, status, meta string) {
	log.Info("43485304HEALTH", checkid, status, meta)
}

func StartBackgroundJob(id, name string, intervalSec int) RunID {
	runid := randomID("BJ", 28)
	fullrunid := id + "." + runid
	b, _ := json.Marshal(&BackgroundJob{Id: id, Name: name, IntervalSec: intervalSec, LastRunId: runid})
	log.Info("2304920394BACKGROUND", "START", string(b))
	return RunID(fullrunid)
}

// status: ok|error|warning|running|outdated
// fullrunid: asdfasdf[.asdfadsf].BJasdlfkjasdf
func ReportBackgroundJob(fullrunid RunID, status string) { //
	ids := strings.Split(string(fullrunid), ".")
	if len(ids) < 2 {
		// not our id
		return
	}

	runId := ids[len(ids)-1]
	jobid := strings.Join(ids[0:len(ids)-1], ".")
	b, _ := json.Marshal(&BackgroundJob{Id: jobid, LastRunId: runId, Status: status})
	log.Info("2304920394BACKGROUND", "STATUS", string(b))
}

const E_track log.E = "track"

// acclient.Track("dupplicated_email", log.M{"title": "duplicated email"})
// acclient.Track("dupplicated_email", log.M{"title": "duplicated email", "noti": false})
func Track(code string, fields ...log.M) {
	var field = log.M{}
	if len(fields) > 0 && fields[0] != nil {
		field = fields[0]
	}

	issue := log.NewError(nil, field, E_track)
	issue.Number = "TRK-" + code
	issue.Class = 0
	b, _ := json.Marshal(issue)
	log.Info("123TRACK", string(b))
}
