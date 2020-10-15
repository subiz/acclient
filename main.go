package acclient

import (
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/cassandra"
	"github.com/subiz/errors"
	"github.com/subiz/goutils/conv"
	pb "github.com/subiz/header/account"
	botpb "github.com/subiz/header/bot"
	clientpb "github.com/subiz/header/client"
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

	authkeyspace    = "auth_auth"
	tableClients    = "mclients"
	tableClientById = "client_by_id"

	botkeyspace = "bizbotbizbot"
	tblBots     = "bots"
)

var (
	readyLock *sync.Mutex
	ready     bool

	cql      *cassandra.Query
	authcql  *cassandra.Query
	convocql *cassandra.Query
	botcql   *cassandra.Query

	cache          *ristretto.Cache
	accthrott      Throttler
	clientthrott   Throttler
	presencethrott Throttler
	botthrott      Throttler
)

func Init(seeds []string) {
	readyLock = &sync.Mutex{}
	go func() {
		readyLock.Lock()
		cql = &cassandra.Query{}
		if err := cql.Connect(seeds, acckeyspace); err != nil {
			panic(err)
		}

		convocql = &cassandra.Query{}
		if err := convocql.Connect(seeds, convokeyspace); err != nil {
			panic(err)
		}

		authcql = &cassandra.Query{}
		if err := authcql.Connect(seeds, authkeyspace); err != nil {
			panic(err)
		}

		botcql = &cassandra.Query{}
		if err := botcql.Connect(seeds, botkeyspace); err != nil {
			panic(err)
		}

		accthrott = NewThrottler(func(key string, payloads []interface{}) {
			getAccountDB(key)
			listAgentsDB(key)
			listGroupsDB(key)
		}, 30000)

		botthrott = NewThrottler(func(accid string, payloads []interface{}) {
			listBotsDB(accid)
		}, 10000)

		presencethrott = NewThrottler(func(key string, payloads []interface{}) {
			listPresencesDB(key)
		}, 5000)

		clientthrott = NewThrottler(func(key string, payloads []interface{}) {
			getClientDB(key)
		}, 30000)

		var err error
		cache, err = ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e4, // number of keys to track frequency of (10k).
			MaxCost:     1e7, // maximum cost of cache (10MB).
			BufferItems: 64,  // number of keys per Get buffer.
		})
		if err != nil {
			panic(err)
		}
		ready = true
		readyLock.Unlock()
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
	waitUntilReady()
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
	if value, found := cache.Get("ACC_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read

		if value == nil {
			return nil, nil
		}
		return value.(*pb.Account), nil
	}
	return getAccountDB(accid)
}

func listAgentsDB(accid string) ([]*pb.Agent, error) {
	waitUntilReady()
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

func listBotsDB(accid string) ([]*botpb.Bot, error) {
	waitUntilReady()
	iter := botcql.Session.Query(`SELECT bot FROM `+tblBots+`WHERE account_id=?`, accid).Iter()
	var botb []byte
	list := make([]*botpb.Bot, 0)
	for iter.Scan(&botb) {
		bot := &botpb.Bot{}
		proto.Unmarshal(botb, bot)
		if bot.GetState() != pb.Agent_deleted.String() {
			list = append(list, bot)
		}
	}
	err := iter.Close()
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, "read all bots", accid)
	}

	cache.SetWithTTL("BOT_"+accid, list, int64(len(list)*1000), 10*time.Second)
	return list, nil
}

func GetAgent(accid, agid string) (*pb.Agent, error) {
	agents, err := ListAgents(accid)
	if err != nil {
		return nil, err
	}

	for _, ag := range agents {
		if ag.GetId() == agid {
			return ag, nil
		}
	}
	return nil, nil
}

func ListAgentsInGroup(accid, groupid string) ([]*pb.Agent, error) {
	groups, err := ListGroups(accid)
	if err != nil {
		return nil, err
	}

	for _, group := range groups {
		if group.GetId() == groupid {
			out := make([]*pb.Agent, 0)
			for _, ag := range group.GetMembers() {
				ag, _ = GetAgent(accid, ag.GetId())
				out = append(out, ag)
			}
			return out, nil
		}
	}
	return nil, nil
}

func ListAgents(accid string) ([]*pb.Agent, error) {
	waitUntilReady()
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
	waitUntilReady()
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
	waitUntilReady()
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
	waitUntilReady()
	iter := cql.Session.Query(`SELECT agent_id FROM `+tblGroupAgent+` WHERE group_id=? AND account_id=? LIMIT 1000`, groupid, accid).Iter()
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

func ListPresences(accid string) ([]*pb.Presence, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("PS_" + accid); found {
		presencethrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.Presence), nil
	}

	presencethrott.Push(accid, nil) // trigger reading from db for future read
	return listPresencesDB(accid)

}

func listPresencesDB(accid string) ([]*pb.Presence, error) {
	waitUntilReady()
	presences := make([]*pb.Presence, 0)
	err := convocql.List(tblPresence, &presences, map[string]interface{}{
		"account_id=": accid,
	}, 1000)
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}

	cache.SetWithTTL("PS_"+accid, presences, int64(len(presences)*20), 10*time.Second)
	return presences, nil
}

func GetClient(id string) (*clientpb.Client, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("CL_" + id); found {
		clientthrott.Push(id, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.(*clientpb.Client), nil
	}

	clientthrott.Push(id, nil) // trigger reading from db for future read
	return getClientDB(id)
}

func getClientDB(id string) (*clientpb.Client, error) {
	waitUntilReady()
	if id == "" {
		return nil, nil
	}

	var accid string
	err := authcql.Session.Query(`SELECT account_id FROM `+tableClientById+
		` WHERE id=?`, id).Scan(&accid)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}

	c := &clientpb.Client{}
	err = authcql.Read(tableClients, c, &clientpb.Client{AccountId: &accid, Id: &id})
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, id, accid)
	}
	cache.SetWithTTL("CL_"+id, c, 1000, 60*time.Second)
	return c, nil
}

func GetBot(accid, botid string) (*botpb.Bot, error) {
	bots, err := ListBots(accid)
	if err != nil {
		return nil, err
	}

	for _, bot := range bots {
		if bot.GetId() == botid {
			return bot, nil
		}
	}
	return nil, nil
}

func ListBots(accid string) ([]*botpb.Bot, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("BOT_" + accid); found {
		botthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.([]*botpb.Bot), nil
	}

	botthrott.Push(accid, nil) // trigger reading from db for future read
	return listBotsDB(accid)
}
