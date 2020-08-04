package acclient

import (
	"github.com/dgraph-io/ristretto"
	"github.com/gocql/gocql"
	"github.com/subiz/cassandra"
	"github.com/subiz/errors"
	"github.com/subiz/goutils/conv"
	pb "github.com/subiz/header/account"
	"sync"
	"time"
)

const (
	acckeyspace   = "accountaccounts"
	tblAccounts   = "accounts"
	tblAgents     = "agents"
	tblPresences  = "presences"
	tblGroups     = "groups"
	tblGroupAgent = "group_agent"

	convokeyspace = "convoconversations"
	tblPresence   = "presence"
)

var (
	readyLock *sync.Mutex
	ready     bool
	session   *gocql.Session
	cql       *cassandra.Query
	cache     *ristretto.Cache

	accthrott Throttler
)

func Init(seeds []string) {
	readyLock := &sync.Mutex{}
	go func() {
		readyLock.Lock()
		cql = &cassandra.Query{}
		if err := cql.Connect(seeds, acckeyspace); err != nil {
			panic(err)
		}
		session = cql.Session

		accthrott = NewThrottler(func(key string, payloads []interface{}) {
			getAccountDB(key)
			listAgentsDB(key)
			listGroupsDB(key)
		}, 30000)
		cache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e4, // number of keys to track frequency of (10k).
			MaxCost:     1e7, // maximum cost of cache (10MB).
			BufferItems: 64,  // number of keys per Get buffer.
		})
		if err != nil {
			panic(err)
		}
		cache = cache
		ready = true
	}()
}

func waitUntilReady() {
	if ready {
		return
	}
	readyLock.Lock()
	readyLock.Unlock()
}

func getAccountDB(id string) (*pb.Account, error) {
	acc := &pb.Account{}
	err := cql.Read(tblAccounts, acc, pb.Account{Id: &id})
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL("ACC_"+id, nil, 1000, 30*time.Second)
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, id)
	}

	cache.SetWithTTL("ACC_"+id, acc, 1000, 30*time.Second)
	return acc, nil
}

func GetAccount(accid string) (*pb.Account, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("AG" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read

		if value == nil {
			return nil, nil
		}
		return value.(*pb.Account), nil
	}
	return getAccountDB(accid)
}

func listAgentsDB(accid string) ([]*pb.Agent, error) {
	var arr = make([]*pb.Agent, 0)
	err := cql.List(tblAgents, &arr, map[string]interface{}{"account_id=": accid}, int(1000))
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, accid)
	}

	list := make([]*pb.Agent, 0)
	for _, a := range arr {
		if a.GetState() != pb.Agent_deleted.String() {
			a.EncryptedPassword = nil
			list = append(list, a)
		}
	}

	cache.SetWithTTL("AG_"+accid, list, int64(len(list)*1000), 30*time.Second)
	return list, nil
}

func ListAgents(accid string) ([]*pb.Agent, error) {
	// cache exists
	if value, found := cache.Get("AG_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.Agent), nil
	}

	accthrott.Push(accid, nil) // trigger reading from db for future read
	return listAgentsDB(accid)
}

func ListGroups(accid string) ([]*pb.AgentGroup, error) {
	// cache exists
	if value, found := cache.Get("GR_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.AgentGroup), nil
	}

	accthrott.Push(accid, nil) // trigger reading from db for future read
	return listGroupsDB(accid)
}

func listGroupsDB(accid string) ([]*pb.AgentGroup, error) {
	var arr = make([]*pb.AgentGroup, 0)
	err := cql.List(tblGroups, &arr, map[string]interface{}{"account_id=": accid}, 1000)
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, accid)
	}

	// list few member in group
	for _, g := range arr {
		agids, _ := listAgentInGroupDB(accid, g.GetId())
		for _, agid := range agids {
			g.Members = append(g.Members, &pb.Agent{Id: conv.S(agid)})
		}

		g.MembersCount = conv.PI32(len(g.Members))
	}
	cache.SetWithTTL("GR_"+accid, arr, int64(len(arr)*1000), 30*time.Second)
	return arr, nil
}

func listAgentInGroupDB(accid, groupid string) ([]string, error) {
	iter := session.Query(`SELECT agent_id FROM `+tblGroupAgent+` WHERE group_id=? LIMIT 1000`, groupid).Iter()
	var ids = make([]string, 0)
	var id string
	for iter.Scan(&id) {
		ids = append(ids, id)
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}
	return ids, nil
}

func ListAgentPresence(accid string) ([]*pb.Presence, error) {
	presences := make([]*pb.Presence, 0)
	err := cql.List(convokeyspace+"."+tblPresence, &presences, map[string]interface{}{
		"account_id=": accid,
	}, 1000)
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}

	cache.SetWithTTL("PS_"+accid, presences, int64(len(presences)*20), 10*time.Second)
	return presences, nil
}
