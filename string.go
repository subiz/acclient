package acclient

import (
	"crypto/md5"
	"encoding/base64"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/header"
)

func Shorten(accid, scope string, val []byte) (string, error) {
	waitUntilReady()

	out := md5.Sum(val)
	hash := base64.StdEncoding.EncodeToString(out[:])

	if _, found := hash_cache.Get(accid + "|" + scope + "|" + hash); found {
		return hash, nil
	}

	err := session.Query("INSERT INTO account.hash_string(accid,scope,hash,value,updated) VALUES(?,?,?,?,?)", accid, scope, hash, val, time.Now().UnixNano()/1e6).Exec()
	if err != nil {
		return hash, header.E500(err, header.E_database_error, accid)
	}
	hash_cache.Set(accid+"|"+scope+"|"+hash, val)
	return hash, nil
}

func Lookup(accid, scope string, hash string) ([]byte, error) {
	waitUntilReady()

	// check cache first
	if value, found := hash_cache.Get(accid + "|" + scope + "|" + hash); found {
		if value == nil {
			return nil, nil
		}
		return value.([]byte), nil
	}

	val := []byte{}
	err := session.Query("SELECT value account.hash_string WHERE accid=? AND scope=? AND hash=?", accid, scope, hash).Scan(&val)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		hash_cache.Set(accid+"|"+scope+"|"+hash, nil)
		return nil, nil
	}

	hash_cache.Set(accid+"|"+scope+"|"+hash, val)
	return val, nil
}
