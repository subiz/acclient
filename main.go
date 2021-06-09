package acclient

import (
	"strings"
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
	pm "github.com/subiz/header/payment"
)

const (
	tblAccounts     = "accounts"
	tblLocale       = "lang"
	tblAgents       = "agents"
	tblPresences    = "presences"
	tblGroups       = "groups"
	tblGroupAgent   = "group_agent"
	tblSubscription = "subs"
	tblPresence     = "presence"
	tblBots         = "bots"
)

var (
	readyLock = &sync.Mutex{}
	ready     bool

	cql      *cassandra.Query
	convocql *cassandra.Query
	botcql   *cassandra.Query

	cache          *ristretto.Cache
	accthrott      *Throttler
	langthrott     *Throttler
	clientthrott   *Throttler
	presencethrott *Throttler
	botthrott      *Throttler
)

func _init() {
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

	langthrott = NewThrottler(func(key string, payloads []interface{}) {
		ks := strings.Split(key, ";")
		if len(ks) != 2 {
			return
		}
		listLocaleMessagesDB(ks[0], ks[1])
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
}

func waitUntilReady() {
	if ready {
		return
	}
	readyLock.Lock()
	if ready {
		readyLock.Unlock()
		return
	}
	_init()
	ready = true
	readyLock.Unlock()
}

func getAccountDB(id string) (*pb.Account, *pm.Subscription, error) {
	waitUntilReady()
	acc := &pb.Account{}
	err := cql.Read(tblAccounts, acc, pb.Account{Id: &id})
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL("ACC_"+id, nil, 1000, 30*time.Second)
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, 500, errors.E_database_error, id)
	}

	cache.SetWithTTL("ACC_"+id, acc, 1000, 30*time.Second)

	sub := &pm.Subscription{}
	err = cql.Read(tblSubscription, sub, pm.Subscription{AccountId: &id})
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL("SUB_"+id, nil, 1000, 30*time.Second)
		return acc, nil, nil
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, 500, errors.E_database_error, id)
	}

	cache.SetWithTTL("SUB_"+id, sub, 1000, 30*time.Second)
	return acc, sub, nil
}

func loadLangDB(accid, locale string, old *header.Lang, fallback bool) (*header.Lang, error) {
	lang := &header.Lang{}
	var message, lastmsg, updatedby, public, category string
	var updated int64
	var k string

	iter := cql.Session.Query(`SELECT k, message, public_state, last_message, updated, author, category FROM `+tblLocale+` WHERE account_id=? AND locale=?`, accid, locale).Iter()
	for iter.Scan(&k, &message, &public, &lastmsg, &updated, &updatedby, &category) {
		if message == "" {
			continue
		}
		found := false
		for _, m := range old.GetMessages() {
			if k == m.Key {
				// only category in locale en-US of acc subiz is valid
				// so this override all categories
				if accid == "subiz" && locale == "en-US" {
					m.Category = category
				}
				found = true
				break
			}
		}

		// add missing key
		if !found {
			lang.Messages = append(lang.Messages, &header.LangMessage{Key: k, FromDefault: fallback, Message: message, PublicState: public, LastMessage: lastmsg, Updated: updated, Author: updatedby, Locale: locale, Category: category})
		}
	}
	if err := iter.Close(); err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error)
	}

	old.Messages = append(old.Messages, lang.Messages...)
	return old, nil
}

func ListLocaleMessageDB(accid, locale string) (*header.Lang, error) {
	lang := &header.Lang{}
	var err error
	// read in custom lang first
	if accid != "subiz" {
		lang, err = loadLangDB(accid, locale, lang, false)
		if err != nil {
			return nil, err
		}
	}
	if locale != "en-US" {
		// fallback to default locale in subiz
		lang, err = loadLangDB("subiz", locale, lang, false)
		if err != nil {
			return nil, err
		}

		// fallback to primary_locale of acount
		acc, err := GetAccount(accid)
		if err != nil {
			return nil, err
		}

		if acc.GetLocale() != "" && acc.GetLocale() != "en-US" && acc.GetLocale() != locale {
			if accid != "subiz" {
				// check to see missing key in the en-US locale - the most completed locale
				lang, err = loadLangDB(accid, acc.GetLocale(), lang, true)
				if err != nil {
					return nil, err
				}
			}

			// fallback to default custom lang
			lang, err = loadLangDB("subiz", acc.GetLocale(), lang, true)
			if err != nil {
				return nil, err
			}
		}
	}

	// finally, fallback to the en-US locale - the most completed locale
	enlang := &header.Lang{}
	isfromdef := locale != "en-US"
	enlang, err = loadLangDB("subiz", "en-US", enlang, isfromdef)
	if err != nil {
		return nil, err
	}

	for _, enmess := range enlang.GetMessages() {
		found := false
		for _, m := range lang.GetMessages() {
			if enmess.Key == m.Key {
				found = true
				m.PublicState = enmess.PublicState
				break
			}
		}

		if !found {
			lang.Messages = append(lang.Messages, enmess)
		}
	}

	lang.AccountId = accid
	lang.Locale = locale
	return lang, nil
}

func listLocaleMessagesDB(accid, locale string) (*header.Lang, error) {
	waitUntilReady()
	lang, err := ListLocaleMessageDB(accid, locale)
	if err != nil {
		return nil, err
	}
	cache.SetWithTTL("LANG_"+accid+"_"+locale, lang, 1000*1000, 30*time.Second)
	return lang, nil
}

// see https://www.localeplanet.com/icu/
func GetLocale(accid, locale string) (*header.Lang, error) {
	if !header.LocaleM[locale] {
		return &header.Lang{}, nil
	}
	if value, found := cache.Get("LANG_" + accid + "_" + locale); found {
		langthrott.Push(accid+";"+locale, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.(*header.Lang), nil
	}

	lang, err := listLocaleMessagesDB(accid, locale)
	if err != nil {
		return nil, err
	}
	return lang, nil
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

	acc, _, err := getAccountDB(accid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(acc).(*pb.Account), nil
}

func GetSubscription(accid string) (*pm.Subscription, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("SUB_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		sub := value.(*pm.Subscription)
		return proto.Clone(sub).(*pm.Subscription), nil
	}

	_, sub, err := getAccountDB(accid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(sub).(*pm.Subscription), nil
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
