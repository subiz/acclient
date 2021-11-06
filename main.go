package acclient

import (
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/gocql/gocql"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/goutils/conv"
	"github.com/subiz/header"
	pb "github.com/subiz/header/account"
	compb "github.com/subiz/header/common"
	n5pb "github.com/subiz/header/noti5"
	pm "github.com/subiz/header/payment"
	"github.com/thanhpk/throttle"
	"google.golang.org/protobuf/proto"
)

const (
	tblAccounts     = "accounts"
	tblLocale       = "lang"
	tblAgents       = "agents"
	tblGroups       = "groups"
	tblGroupAgent   = "group_agent"
	tblSubscription = "subs"
	tblPresence     = "convo.presence"
	tblBots         = "bizbot.bots"
)

var (
	readyLock = &sync.Mutex{}
	ready     bool

	session *gocql.Session

	cache           *ristretto.Cache
	accthrott       *throttle.Throttler
	langthrott      *throttle.Throttler
	clientthrott    *throttle.Throttler
	presencethrott  *throttle.Throttler
	botthrott       *throttle.Throttler
	n5settingthrott *throttle.Throttler
)

func _init() {
	cluster := gocql.NewCluster("db-0")
	cluster.Timeout = 60 * time.Second
	cluster.ConnectTimeout = 60 * time.Second
	cluster.Keyspace = "account"
	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	accthrott = throttle.NewThrottler(func(accid string, payloads []interface{}) {
		getAccountDB(accid)
		listAgentsDB(accid)
		listGroupsDB(accid)
		listAttrDefsDB(accid)
		getShopSettingDb(accid)
	}, 60000)

	langthrott = throttle.NewThrottler(func(key string, payloads []interface{}) {
		ks := strings.Split(key, ";")
		if len(ks) != 2 {
			return
		}
		listLocaleMessagesDB(ks[0], ks[1])
	}, 30000)

	botthrott = throttle.NewThrottler(func(accid string, payloads []interface{}) {
		listBotsDB(accid)
	}, 20000)

	n5settingthrott = throttle.NewThrottler(func(key string, payloads []interface{}) {
		ks := strings.Split(key, ";")
		if len(ks) != 2 {
			return
		}

		accid, agid := ks[0], ks[1]
		getNotificationSettingDB(accid, agid)
	}, 30000)

	presencethrott = throttle.NewThrottler(func(key string, payloads []interface{}) {
		listPresencesDB(key)
	}, 10000)

	cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5, // number of keys to track frequency of (100k).
		MaxCost:     4e7, // maximum cost of cache (40MB).
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
	var businesshourb, feature, force_feature, leadsetting, monitorsetting []byte
	var supportedlocales []string
	var address, city, country, dateformat, facebook, lang, locale, logo_url, name, ownerid, phone, referrer_from, region, state, tax_id, timezone, twitter, url string
	var confirmed, created, lasttokenrequested, modified int64
	var zipcode int32
	var currency string
	var currency_locked bool

	err := session.Query("SELECT address, business_hours,city,confirmed, country, created,date_format, facebook, feature, force_feature, lang, last_token_requested, lead_setting, locale, logo_url, modified, name, owner_id, phone, referrer_from, region, state, supported_locales, tax_id, timezone, twitter, url, webpage_monitor_setting, zip_code, currency, currency_locked FROM account.accounts WHERE id=?", id).Scan(&address, &businesshourb, &city, &confirmed, &country, &created, &dateformat, &facebook, &feature, &force_feature, &lang, &lasttokenrequested, &leadsetting, &locale, &logo_url, &modified, &name, &ownerid, &phone, &referrer_from, &region, &state, &supportedlocales, &tax_id, &timezone, &twitter, &url, &monitorsetting, &zipcode, &currency, &currency_locked)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL("ACC_"+id, nil, 1000, 30*time.Second)
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, header.E500(err, header.E_database_error, id)
	}

	ms := &pb.WebpageMonitorSetting{}
	proto.Unmarshal(monitorsetting, ms)

	ls := &pb.LeadSetting{}
	proto.Unmarshal(leadsetting, ls)

	f := &pb.Feature{}
	ff := &pb.Feature{}
	proto.Unmarshal(feature, f)
	proto.Unmarshal(force_feature, ff)

	bh := &pb.BusinessHours{}
	proto.Unmarshal(businesshourb, bh)
	acc := &pb.Account{
		Id:                    &id,
		Address:               &address,
		BusinessHours:         bh,
		City:                  &city,
		Confirmed:             &confirmed,
		Country:               &country,
		Created:               &created,
		DateFormat:            &dateformat,
		Facebook:              &facebook,
		Feature:               f,
		ForceFeature:          ff,
		Lang:                  &lang,
		LastTokenRequested:    &lasttokenrequested,
		LeadSetting:           ls,
		Locale:                &locale,
		LogoUrl:               &logo_url,
		Modified:              &modified,
		Name:                  &name,
		OwnerId:               &ownerid,
		Phone:                 &phone,
		ReferrerFrom:          &referrer_from,
		State:                 &state,
		SupportedLocales:      supportedlocales,
		TaxId:                 &tax_id,
		Timezone:              &timezone,
		Twitter:               &twitter,
		Url:                   &url,
		WebpageMonitorSetting: ms,
		ZipCode:               &zipcode,
		Currency:              &currency,
		CurrencyLocked:        &currency_locked,
	}
	cache.SetWithTTL("ACC_"+id, acc, 1000, 60*time.Second)

	var autocharge, autorenew bool
	var subname, plan, pmmethod, promo, referralby string
	var subcreated, ended, started int64
	var billingcyclemonth, next_billing_cycle_month uint32
	var credit float32
	var customerb, limitb []byte
	notebs := make([][]byte, 0)

	err = session.Query("SELECT auto_charge, auto_renew, billing_cycle_month, created, credit, customer, ended, \"limit\", name, next_billing_cycle_month, notes, plan, primary_payment_method, promotion_code, referral_by, started FROM account.subs WHERE account_id=?", id).Scan(
		&autocharge, &autorenew, &billingcyclemonth, &subcreated, &credit, &customerb, &ended, &limitb, &subname, &next_billing_cycle_month, &notebs, &plan, &pmmethod, &promo, &referralby, &started)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL("SUB_"+id, nil, 1000, 30*time.Second)
		return acc, nil, nil
	}
	if err != nil {
		return nil, nil, header.E500(err, header.E_database_error, id)
	}

	limit := &pm.Limit{}
	proto.Unmarshal(limitb, limit)

	notes := []*pm.Note{}
	for _, noteb := range notebs {
		note := &pm.Note{}
		proto.Unmarshal(noteb, note)
		notes = append(notes, note)
	}

	customer := &pm.Customer{}
	proto.Unmarshal(customerb, customer)
	sub := &pm.Subscription{
		AccountId:             &id,
		AutoRenew:             &autorenew,
		BillingCycleMonth:     &billingcyclemonth,
		Created:               &subcreated,
		Credit:                &credit,
		Customer:              customer,
		Ended:                 &ended,
		Limit:                 limit,
		Name:                  &subname,
		Notes:                 notes,
		Plan:                  &plan,
		NextBillingCycleMonth: &next_billing_cycle_month,
		PrimaryPaymentMethod:  &pmmethod,
		PromotionCode:         &promo,
		ReferralBy:            &referralby,
		Started:               &started,
	}
	cache.SetWithTTL("SUB_"+id, sub, 1000, 30*time.Second)
	return acc, sub, nil
}

func getShopSettingDb(id string) (*header.ShopSetting, error) {
	waitUntilReady()
	var data = []byte{}
	err := session.Query("SELECT data FROM account.shop_setting WHERE account_id=?", id).Scan(&data)
	setting := &header.ShopSetting{}

	if err != nil && err.Error() != gocql.ErrNotFound.Error() {
		return nil, header.E500(err, header.E_database_error, id)
	}
	if len(data) > 0 {
		proto.Unmarshal(data, setting)
	}

	poses := []*header.POS{}
	// read pos
	data = []byte{}
	iter := session.Query(`SELECT data FROM account.pos WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		pos := header.POS{}
		proto.Unmarshal(data, &pos)
		poses = append(poses, &pos)
	}
	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error)
	}

	setting.Poses = poses
	setting.AccountId = id

	cache.SetWithTTL("SHOPSETTING_"+id, setting, 1000, 60*time.Second)
	return setting, nil
}

func loadLangDB(accid, locale string, old *header.Lang, fallback bool) (*header.Lang, error) {
	lang := &header.Lang{}
	var message, lastmsg, updatedby, public, category string
	var updated int64
	var k string

	iter := session.Query(`SELECT k, message, public_state, last_message, updated, author, category FROM `+tblLocale+` WHERE account_id=? AND locale=?`, accid, locale).Iter()
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
		return nil, header.E500(err, header.E_database_error)
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

func GetNotificationSetting(accid, agid string) (*n5pb.Setting, error) {
	waitUntilReady()

	if value, found := cache.Get("N5Setting_" + accid + "_" + agid); found {
		n5settingthrott.Push(accid+";"+agid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		setting := value.(*n5pb.Setting)
		return proto.Clone(setting).(*n5pb.Setting), nil
	}

	setting, err := getNotificationSettingDB(accid, agid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(setting).(*n5pb.Setting), nil

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

	var id, avatar_url, client_id, country_code, email, encrypted_password, fullname, gender string
	var invited_by, jobtitle, lang, phone, state, typ, tz string
	var isowner, issupervisor bool
	var scopes []string
	var joined, lasttokenrequested, passwordchanged, seen int64
	var dashboard_setting []byte
	var modified int64

	iter := session.Query("SELECT id, avatar_url, client_id, country_code, dashboard_setting, email, encrypted_password, fullname, gender, invited_by, is_owner, is_supervisor, job_title, joined, lang, last_token_requested, modified, password_changed, phone, scopes, state, type, timezone, seen FROM account.agents where account_id=?", accid).Iter()
	for iter.Scan(&id, &avatar_url, &client_id, &country_code, &dashboard_setting, &email, &encrypted_password, &fullname, &gender, &invited_by,
		&isowner, &issupervisor, &jobtitle, &joined, &lang, &lasttokenrequested, &modified, &passwordchanged, &phone, &scopes, &state, &typ, &tz, &seen) {
		ds := &pb.DashboardAgent{}
		proto.Unmarshal(dashboard_setting, ds)
		ag := &pb.Agent{
			AccountId:          conv.S(accid),
			Id:                 conv.S(id),
			AvatarUrl:          conv.S(avatar_url),
			ClientId:           conv.S(client_id),
			CountryCode:        conv.S(country_code),
			DashboardSetting:   ds,
			Email:              conv.S(email),
			EncryptedPassword:  conv.S(encrypted_password),
			Fullname:           conv.S(fullname),
			Gender:             conv.S(gender),
			InvitedBy:          conv.S(invited_by),
			IsOwner:            conv.B(isowner),
			IsSupervisor:       conv.B(issupervisor),
			JobTitle:           conv.S(jobtitle),
			Joined:             conv.PI64(int(joined)),
			Lang:               conv.S(lang),
			LastTokenRequested: conv.PI64(int(lasttokenrequested)),
			Modified:           conv.PI64(int(modified)),
			PasswordChanged:    conv.PI64(int(passwordchanged)),
			Phone:              conv.S(phone),
			Scopes:             scopes,
			State:              conv.S(state),
			Type:               conv.S(typ),
			Timezone:           conv.S(tz),
			Seen:               conv.PI64(int(seen)),
		}
		scopes = make([]string, 0)
		arr = append(arr, ag)
	}

	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error, accid)
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

func listAttrDefsDB(accid string) (map[string]*header.AttributeDefinition, error) {
	defs := make(map[string]*header.AttributeDefinition, 0)

	iter := session.Query("SELECT key, description, kind, list_items, name, type, updated FROM user.attribute_definitions WHERE account_id=? LIMIT 1000", accid).Iter()
	key, desc, kind, name, typ := "", "", "", "", ""
	var updated int64
	list_items := make([]string, 0)
	for iter.Scan(&key, &desc, &kind, &list_items, &name, &typ, &updated) {
		list := make([]string, 0)
		for _, item := range list_items {
			list = append(list, item)
		}
		defs[key] = &header.AttributeDefinition{
			AccountId:   accid,
			Key:         key,
			Description: desc,
			Kind:        kind,
			Name:        name,
			Type:        typ,
			ListItems:   list_items,
			Updated:     clock.UnixMili(updated),
		}
	}
	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error)
	}

	defaults := ListDefaultDefs()
	for _, a := range defaults {
		defs[a.Key] = a
	}

	cache.SetWithTTL("ATTRDEF_"+accid, defs, int64(len(defs)*1000), 60*time.Second)
	return defs, nil
}

func getNotificationSettingDB(accid, agid string) (*n5pb.Setting, error) {
	waitUntilReady()

	dnd, em, mobile, web := make([]byte, 0), make([]byte, 0), make([]byte, 0), make([]byte, 0)
	var instant_mute_until, updated int64
	err := session.Query(`SELECT do_not_disturb, email, instant_mute_until, mobile, updated, web FROM noti5.notisettings WHERE account_id=? AND agent_id=?`, accid, agid).Scan(&dnd, &em, &instant_mute_until, &mobile, &updated, &web)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		// default setting
		setting := &n5pb.Setting{
			AccountId: &accid, AgentId: &agid,
			Web: &n5pb.Subscription{
				NewMessage:            conv.B(true),
				UserCreated:           conv.B(true),
				UserReturned:          conv.B(true),
				CampaignUserConverted: conv.B(true),
				UserOpenedEmail:       conv.B(true),
			},
			Mobile: &n5pb.Subscription{NewMessage: conv.B(true)},
		}
		cache.SetWithTTL("N5Setting_"+accid+"_"+agid, setting, 1000, 30*time.Second)
		return setting, nil
	}
	if err != nil {
		return nil, header.E500(err, header.E_database_error, "read noti setting", accid, agid)
	}

	dnds := &n5pb.DoNotDisturb{}
	proto.Unmarshal(dnd, dnds)

	webs := &n5pb.Subscription{}
	proto.Unmarshal(web, webs)
	mobiles := &n5pb.Subscription{}
	proto.Unmarshal(mobile, mobiles)
	email := &n5pb.Subscription{}
	proto.Unmarshal(em, email)

	setting := &n5pb.Setting{
		AccountId:        &accid,
		AgentId:          &agid,
		DoNotDisturb:     dnds,
		InstantMuteUntil: &instant_mute_until,
		Updated:          &updated,
		Web:              webs,
		Mobile:           mobiles,
		Email:            email,
	}

	cache.SetWithTTL("N5Setting_"+accid+"_"+agid, setting, 1000, 30*time.Second)
	return setting, nil
}

func listBotsDB(accid string) ([]*header.Bot, error) {
	waitUntilReady()
	iter := session.Query(`SELECT bot FROM `+tblBots+` WHERE account_id=?`, accid).Iter()
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
		return nil, header.E500(err, header.E_database_error, "read all bots", accid)
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

	iter := session.Query("SELECT id, created, logo_url, modified, name FROM account.groups WHERE account_id=? LIMIT 500", accid).Iter()
	var id, name, logourl string
	var created, modified int64
	for iter.Scan(&id, &created, &logourl, &modified, &name) {
		group := &pb.AgentGroup{
			AccountId: conv.S(accid),
			Id:        conv.S(id),
			Created:   conv.PI64(int(created)),
			Modified:  conv.PI64(int(modified)),
			Name:      conv.S(name),
		}
		arr = append(arr, group)
	}
	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error, accid)
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
	iter := session.Query(`SELECT agent_id FROM `+tblGroupAgent+` WHERE group_id=? AND account_id=? LIMIT 1000`, groupid, accid).Iter()
	var ids = make([]string, 0)
	var id string
	for iter.Scan(&id) {
		ids = append(ids, id)
	}
	if err := iter.Close(); err != nil {
		return nil, header.E500(err, header.E_database_error, accid, groupid)
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

	iter := session.Query(`SELECT user_id, ip, pinged, ua FROM `+tblPresence+` WHERE account_id=? LIMIT 1000`, accid).Iter()
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
		return nil, header.E500(err, header.E_database_error)
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

func ListDefs(accid string) (map[string]*header.AttributeDefinition, error) {
	waitUntilReady()
	if value, found := cache.Get("ATTRDEF_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.AttributeDefinition), nil
	}

	accthrott.Push(accid, nil) // trigger reading from db for future read
	return listAttrDefsDB(accid)
}

func GetShopSetting(accid string) (*header.ShopSetting, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("SHOPSETTING_" + accid); found {
		accthrott.Push(accid, nil) // trigger reading from db for future read

		if value == nil {
			return nil, nil
		}
		setting := value.(*header.ShopSetting)
		return proto.Clone(setting).(*header.ShopSetting), nil
	}

	setting, err := getShopSettingDb(accid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(setting).(*header.ShopSetting), nil
}

func ConvertMoney(accid string, price *header.Price) (*header.Price, error) {
	acc, err := GetAccount(accid)
	if err != nil {
		return nil, err
	}
	setting, err := GetShopSetting(accid)
	if err != nil {
		return nil, err
	}

	cur := strings.TrimSpace(acc.GetCurrency())
	if cur == "" {
		return nil, header.E400(nil, header.E_invalid_base_currency, "empty")
	}

	price = proto.Clone(price).(*header.Price)
	// first, calculate fpv
	fpv := header.CalcFPV(price, cur)
	for _, cur := range setting.GetSupportedCurrencies() {
		if cur.GetRate() >= 0 {
			money := float32(fpv) * cur.GetRate() / 1000000
			header.SetCurrency(price, cur.GetCode(), money)
		}
	}
	return price, nil
}

// ConvertMoney(accid, product.GetPrice())
// ConvertMoney(accid, &header.Price{
//   VND: 323000,
//   currency: "VND",
// })
//
// => header.Price{
//    currency: "VND",
//    VND: 323000,
//    USD: 20,
//    FPV: 20000000,
// }
