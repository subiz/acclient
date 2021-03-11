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
	"github.com/subiz/header"
	pb "github.com/subiz/header/account"
	compb "github.com/subiz/header/common"
)

const (
	tblAccounts   = "accounts"
	tblAgents     = "agents"
	tblPresences  = "presences"
	tblGroups     = "groups"
	tblGroupAgent = "group_agent"

	tblPresence   = "presence"
	tblBots     = "bots"
)

var (
	readyLock = &sync.Mutex{}
	ready     bool

	cql      *cassandra.Query
	convocql *cassandra.Query
	botcql   *cassandra.Query

	cache          *ristretto.Cache
	accthrott      Throttler
	clientthrott   Throttler
	presencethrott Throttler
	botthrott      Throttler
)

func Init() {
	go func() {
		readyLock.Lock()
		cql = &cassandra.Query{}
		if err := cql.Connect([]string{"db-0"}, "account"); err != nil {
			panic(err)
		}

		convocql = &cassandra.Query{}
		if err := convocql.Connect([]string{"db-0"}, "convo"); err != nil {
			panic(err)
		}

		botcql = &cassandra.Query{}
		if err := botcql.Connect([]string{"db-0"}, "bizbot"); err != nil {
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

// TODO return proto clone of other methods
func GetAccount(accid string) (*pb.Account, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("ACC_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read

		if value == nil {
			return nil, nil
		}
		acc := value.(*pb.Account)
		return proto.Clone(acc).(*pb.Account), nil
	}

	acc, err := getAccountDB(accid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(acc).(*pb.Account), nil
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

func listBotsDB(accid string) ([]*header.Bot, error) {
	waitUntilReady()
	iter := botcql.Session.Query(`SELECT bot FROM `+tblBots+` WHERE account_id=?`, accid).Iter()
	var botb []byte
	list := make([]*header.Bot, 0)
	for iter.Scan(&botb) {
		bot := &header.Bot{}
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

	bots, err := ListBots(accid)
	if err != nil {
		return nil, err
	}

	for _, bot := range bots {
		if bot.GetId() == agid {
			return Bot2Agent(bot), nil
		}
	}

	return nil, nil
}

func Bot2Agent(bot *header.Bot) *pb.Agent {
	return &pb.Agent{
		AccountId: &bot.AccountId,
		Id:        &bot.Id,
		State:     &bot.State,
		AvatarUrl: &bot.AvatarUrl,
		Fullname:  &bot.Fullname,
		Type:      conv.S(compb.Type_bot),
		Modified:  &bot.Updated,
		Joined:    &bot.Created,
		InvitedBy: &bot.CreatedBy,
	}
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

	agents := []*pb.Agent{}
	// cache exists
	if value, found := cache.Get("AG_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		agents = value.([]*pb.Agent)
	} else {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		var err error
		agents, err = listAgentsDB(accid)
		if err != nil {
			return nil, err
		}
	}

	bots, err := ListBots(accid)
	if err != nil {
		return nil, err
	}

	for _, bot := range bots {
		agents = append(agents, Bot2Agent(bot))
	}
	return agents, nil
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

	iter := convocql.Session.Query(`SELECT user_id, ip, pinged, ua FROM `+tblPresence+` WHERE account_id=? LIMIT 1000`, accid).Iter()
	uid, ip, ua := "", "", ""
	pinged := int64(0)
	for iter.Scan(&uid, &ip, &pinged, &ua) {
		presences = append(presences, &pb.Presence{
			AccountId: conv.S(accid),
			UserId:    conv.S(uid),
			Ip:        conv.S(ip),
			Pinged:    conv.PI64(int(pinged)),
			Ua:        conv.S(ua),
		})
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}

	cache.SetWithTTL("PS_"+accid, presences, int64(len(presences)*20), 10*time.Second)
	return presences, nil
}

func GetBot(accid, botid string) (*header.Bot, error) {
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

func ListBots(accid string) ([]*header.Bot, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("BOT_" + accid); found {
		botthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.([]*header.Bot), nil
	}

	botthrott.Push(accid, nil) // trigger reading from db for future read
	return listBotsDB(accid)
}
