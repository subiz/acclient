package acclient

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/header"
	"github.com/subiz/idgen"
	"github.com/subiz/log"
)

// CREATE TABLE account.job (accid ascii, id ascii, name text, desscription text, category text, timeout_sec bigint, created bigint, force_ended bigint, ended bigint, status text, status_updated bigint, output blob, PRIMARY KEY ((accid, id)));

func GetJob(accid, jobid string) *header.Job {
	var name, description, category, status string
	var timeout_sec, created, force_ended, ended, status_updated, last_ping_ms int64
	output := []byte{}
	for {
		err := session.Query(`SELECT name, description, category, timeout_sec, created, force_ended, ended, status, status_updated, output, last_ping_ms FROM account.job WHERE accid=? AND id=?`, accid, jobid).Scan(&name, &description, &category, &timeout_sec, &created, &force_ended, &ended, &status, &status_updated, &output, &last_ping_ms)
		if err != nil && err.Error() == gocql.ErrNotFound.Error() {
			return nil
		}

		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "jobid": jobid})
			time.Sleep(5 * time.Second)
			continue
		}

		if ended == 0 && time.Now().UnixMilli() > created+timeout_sec*1000 {
			ended = created + timeout_sec*1000
			if force_ended == 0 {
				force_ended = ended
			}
		}

		return &header.Job{
			AccountId:     accid,
			Id:            jobid,
			Name:          name,
			Description:   description,
			Category:      category,
			TimeoutSec:    timeout_sec,
			Created:       created,
			ForceEnded:    force_ended,
			Ended:         ended,
			Status:        status,
			StatusUpdated: status_updated,
			Output:        output,
			LastPingMs:    last_ping_ms,
		}
	}
}

func StartJob(accid, name, description, category string, timeoutsec int64) string {
	waitUntilReady()
	jobid := idgen.NewJobId()
	for i := 0; i < 1000; i++ {
		err := session.Query(`INSERT INTO account.job(accid, id, name, description, category, timeout_sec, created) VALUES(?,?,?,?,?,?,?) TTL 864000`, accid, jobid, name, description, category, timeoutsec, time.Now().UnixMilli()).Exec()
		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "name": name, "description": description, "category": category})
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}
	return jobid
}

func UpdateJobStatus(accid, jobid, status string) {
	updated := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		err := session.Query(`INSERT INTO account.job(accid, id, status, status_updated) VALUES(?,?,?,?) TTL 864000`, accid, jobid, status, updated).Exec()
		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "jobid": jobid, "status": status})
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}
}

// force end -> status code -5
func ForceEndJob(accid, jobid string) {
	ended := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		err := session.Query(`INSERT INTO account.job(accid, id, force_ended, ended) VALUES(?,?,?,?) TTL 864000`, accid, jobid, ended, ended).Exec()
		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "job_id": jobid})
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}
}

func EndJob(accid, jobid, status string, output []byte) {
	ended := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		err := session.Query(`INSERT INTO account.job(accid, id, status, ended, output, last_ping_ms) VALUES(?,?,?,?,?,?) TTL 864000`, accid, jobid, status, ended, output, ended).Exec()
		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "jobid": jobid, "status": status})
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}
}

// return ended or job status
func PingJob(accid, jobid string) string {
	ping := time.Now().UnixMilli()
	var status string
	var timeout_sec, created, force_ended, ended, last_ping_ms int64

	for i := 0; i < 1000; i++ {
		err := session.Query(`SELECT timeout_sec, created, force_ended, ended, status, last_ping_ms FROM account.job WHERE accid=? AND id=?`, accid, jobid).Scan(&timeout_sec, &created, &force_ended, &ended, &status, &last_ping_ms)
		if err != nil && err.Error() == gocql.ErrNotFound.Error() {
			return "ended"
		}

		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "jobid": jobid})
			time.Sleep(5 * time.Second)
			continue
		}

		if ended == 0 && time.Now().UnixMilli() > created+timeout_sec*1000 {
			ended = created + timeout_sec*1000
			if force_ended == 0 {
				force_ended = ended
			}
		}

		err = session.Query(`INSERT INTO account.job(accid, id, last_ping_ms) VALUES(?,?,?) TTL 864000`, accid, jobid, ping).Exec()
		if err != nil {
			log.ERetry(err, log.M{"account_id": accid, "jobid": jobid})
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}

	if ended > 0 {
		return "ended"
	}

	return status
}
