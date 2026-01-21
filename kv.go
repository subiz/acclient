package acclient

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/subiz/log"
)

// Get returns the value matched the provided key
// Note that this function dont return error when the value is not existed. Instead,
// it returns an empty value "" and a boolean `true` indicate that the value is empty
//
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Set("user", "324234", "onetwothree")
//
//	kvclient.Get("user", "324234") => "onetwothree"
func GetKV(scope, key string) (string, bool, error) {
	waitUntilReady()
	key = scope + "@" + key
	var val string
	err := session.Query(`SELECT v FROM kv.kv WHERE k=?`, key).Scan(&val)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return "", false, nil
	}

	if err != nil {
		return "", false, log.EServer(context.Background(), "", err, "scope", scope, "key", key)
	}

	return val, true, nil
}

// Set puts a new key-value pair to the database
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Set("user", "324234", "onetwothree")
// E.g: kvclient.Set("account", "324234", "onetwothree")
func SetKV(scope, key, value string) error {
	waitUntilReady()
	key = scope + "@" + key
	// ttl 60 days
	err := session.Query(`INSERT INTO kv.kv(k,v) VALUES(?,?) USING TTL 5184000`, key, value).Exec()
	if err != nil {
		return log.EServer(context.Background(), "", err, "scope", scope, "key", key, "value", value)
	}

	return nil
}

// Set puts a new key-value pair to the database
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Set("user", "324234", "onetwothree")
// E.g: kvclient.Set("account", "324234", "onetwothree")
func SetKVTTL(scope, key, value string, ttlsec int) error {
	waitUntilReady()
	key = scope + "@" + key
	// ttl 60 days
	err := session.Query(`INSERT INTO kv.kv(k,v) VALUES(?,?) USING TTL ?`, key, value, ttlsec).Exec()
	if err != nil {
		return log.EServer(context.Background(), "", err, "scope", scope, "key", key, "value", value, "ttl_sec", ttlsec)
	}

	return nil
}

// Del removes key from the database
// scope is a required paramenter, used as a namespace to prevent collision between
// multiple services while using this lib concurrently.
// E.g: kvclient.Del("user", "324234")
func DelKV(scope, key string) error {
	waitUntilReady()
	key = scope + "@" + key
	err := session.Query(`DELETE FROM kv.kv WHERE k=?`, key).Exec()
	if err != nil {
		return log.EServer(context.Background(), "", err, "scope", scope, "key", key)
	}
	return nil
}
