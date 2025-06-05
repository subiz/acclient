package acclient

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/goutils/business_hours"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/goutils/conv"
	"github.com/subiz/header"
	pb "github.com/subiz/header/account"
	compb "github.com/subiz/header/common"
	pm "github.com/subiz/header/payment"
	"github.com/subiz/idgen"
	"github.com/subiz/kafka"
	"github.com/subiz/log"
	"github.com/thanhpk/ascii"
	gocache "github.com/thanhpk/go-cache"
	"github.com/thanhpk/randstr"
	"google.golang.org/protobuf/proto"
)

var (
	readyLock  = &sync.Mutex{}
	missingAcc = map[string]bool{}

	ready    bool
	hostname string

	session            *gocql.Session
	accmgr             header.AccountMgrClient
	creditmgr          header.CreditMgrClient
	registryClient     header.NumberRegistryClient
	numpubsub          header.PubsubClient
	cache              = gocache.New(60 * time.Minute)
	hash_cache         *gocache.Cache
	creditCache        = gocache.New(60 * time.Second) // accid+"."+creditid
	subscribeTopicLock = &sync.Mutex{}
	subscribeTopics    = map[string]bool{}
)

var EACCESS_DENY = log.NewError(nil, log.M{"no_report": true}, log.E_access_deny)

func _init() {
	hostname, _ = os.Hostname()
	session = header.ConnectDB([]string{"db-0"}, "account")
	conn := header.DialGrpc("account-0.account:10283", header.WithShardRedirect())
	accmgr = header.NewAccountMgrClient(conn)
	creditmgr = header.NewCreditMgrClient(conn)

	conn = header.DialGrpc("numreg-0.numreg:8665")
	registryClient = header.NewNumberRegistryClient(conn)
	numpubsub = header.NewPubsubClient(conn)
	go pollLoop()
	hash_cache = gocache.New(10 * time.Minute)
}

// for testing purpose
func ClearCache() {
	cache = gocache.New(60 * time.Minute)
	hash_cache = gocache.New(10 * time.Minute)
	creditCache = gocache.New(60 * time.Second) // accid+"."+creditid
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

func getAccountDB(id string) (*pb.Account, error) {
	subscribe(id, "account")
	waitUntilReady()

	readyLock.Lock()
	if missingAcc[id] {
		readyLock.Unlock()
		return nil, nil
	}
	readyLock.Unlock()

	account, err := accmgr.GetAccount(header.ToGrpcCtx(&compb.Context{Credential: &compb.Credential{Issuer: hostname, AccountId: id, Type: compb.Type_subiz}}), &header.Id{AccountId: id, Id: id})
	if err == nil && account != nil {
		cache.Set("account."+id, account)
		return account, nil
	}

	if log.IsErr(err, log.E_missing_resource.String()) {
		cache.Set("account."+id, nil)
		readyLock.Lock()
		missingAcc[id] = true
		readyLock.Unlock()

		return nil, nil
	}

	var businesshourb, invoice_infob, leadsetting, userattributesetting []byte
	var supportedlocales []string
	var address, city, country, dateformat, lang, locale, logo_url, logo_url_128, name, ownerid, phone, referrer_from, state, timezone, url string
	var created, modified int64
	var zipcode int32
	var currency string
	var currency_locked bool
	err = session.Query("SELECT address, business_hours,city, country, created, date_format, lang, lead_setting, user_attribute_setting, locale, logo_url, logo_url_128, modified, name, owner_id, phone, referrer_from, state, supported_locales, timezone, url, zip_code, currency, currency_locked, invoice_info FROM account.accounts WHERE id=?", id).Scan(&address, &businesshourb, &city, &country, &created, &dateformat, &lang, &leadsetting, &userattributesetting, &locale, &logo_url, &logo_url_128, &modified, &name, &ownerid, &phone, &referrer_from, &state, &supportedlocales, &timezone, &url, &zipcode, &currency, &currency_locked, &invoice_infob)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.Set("account."+id, nil)
		readyLock.Lock()
		missingAcc[id] = true
		readyLock.Unlock()
		return nil, nil
	}

	if err != nil {
		return nil, log.ERetry(err, log.M{"id": id})
	}

	ii := &pb.InvoiceInfo{}
	proto.Unmarshal(invoice_infob, ii)

	uas := &pb.UserAttributeSetting{}
	proto.Unmarshal(userattributesetting, uas)

	ls := &pb.LeadSetting{}
	proto.Unmarshal(leadsetting, ls)

	bh := &pb.BusinessHours{}
	proto.Unmarshal(businesshourb, bh)
	acc := &pb.Account{
		Id:                   &id,
		Address:              &address,
		BusinessHours:        bh,
		City:                 &city,
		Country:              &country,
		Created:              &created,
		DateFormat:           &dateformat,
		Lang:                 &lang,
		LeadSetting:          ls,
		Locale:               &locale,
		LogoUrl:              &logo_url,
		LogoUrl_128:          &logo_url_128,
		Modified:             &modified,
		Name:                 &name,
		OwnerId:              &ownerid,
		Phone:                &phone,
		ReferrerFrom:         &referrer_from,
		State:                &state,
		SupportedLocales:     supportedlocales,
		Timezone:             &timezone,
		Url:                  &url,
		ZipCode:              &zipcode,
		Currency:             &currency,
		CurrencyLocked:       &currency_locked,
		UserAttributeSetting: uas,
		InvoiceInfo:          ii,
	}
	cache.Set("account."+id, acc)
	return acc, nil
}

func getSubDB(id string) (*pm.Subscription, error) {
	subscribe(id, "subscription")
	waitUntilReady()
	var limitb []byte
	var autocharge bool
	var plan, promo string
	var subcreated, churned, ended, started, fpv_unlimited_agent_price int64
	var billingcyclemonth, next_billing_cycle_month uint32
	var credit float32
	var use_ticket int64

	err := session.Query("SELECT \"limit\", auto_charge, billing_cycle_month, created, credit, ended, churned, next_billing_cycle_month, plan, promotion_code, started, fpv_unlimited_agent_price, use_ticket FROM account.subs WHERE account_id=?", id).Scan(
		&limitb, &autocharge, &billingcyclemonth, &subcreated, &credit, &ended, &churned, &next_billing_cycle_month, &plan, &promo, &started, &fpv_unlimited_agent_price, &use_ticket)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.Set("subscription."+id, nil)
		return nil, nil
	}
	if err != nil {
		return nil, log.ERetry(err, log.M{"account_id": id})
	}

	limit := &compb.Limit{}
	proto.Unmarshal(limitb, limit)

	sub := &pm.Subscription{
		AccountId:              &id,
		BillingCycleMonth:      &billingcyclemonth,
		Created:                &subcreated,
		Credit:                 &credit,
		Limit:                  limit,
		Ended:                  &ended,
		Churned:                &churned,
		Plan:                   &plan,
		NextBillingCycleMonth:  &next_billing_cycle_month,
		PromotionCode:          &promo,
		Started:                &started,
		FpvUnlimitedAgentPrice: &fpv_unlimited_agent_price,
		UseTicket:              conv.PI64(int(use_ticket)),
	}
	cache.Set("subscription."+id, sub)
	return sub, nil
}

func getShopSettingDb(id string) (*header.ShopSetting, error) {
	waitUntilReady()
	subscribe(id, "shop_setting")
	var data = []byte{}
	err := session.Query("SELECT data FROM account.shop_setting WHERE account_id=?", id).Scan(&data)
	setting := &header.ShopSetting{}

	if err != nil && err.Error() != gocql.ErrNotFound.Error() {
		return nil, log.ERetry(err, log.M{"id": id})
	}
	if len(data) > 0 {
		proto.Unmarshal(data, setting)
	}

	shopAddresses := []*header.Address{}
	// read pos
	data = []byte{}
	iter := session.Query(`SELECT data FROM account.shop_address WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		shopAddress := header.Address{}
		proto.Unmarshal(data, &shopAddress)
		shopAddresses = append(shopAddresses, &shopAddress)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"id": id})
	}

	taxes := []*header.Tax{}
	// read pos
	data = []byte{}
	iter = session.Query(`SELECT data FROM account.tax WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		tax := header.Tax{}
		proto.Unmarshal(data, &tax)
		taxes = append(taxes, &tax)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"id": id})
	}

	paymentmethods := []*header.PaymentMethod{}
	// read pos
	data = []byte{}
	iter = session.Query(`SELECT data FROM account.payment_method WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		pm := header.PaymentMethod{}
		proto.Unmarshal(data, &pm)
		paymentmethods = append(paymentmethods, &pm)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"id": id})
	}

	shops := make([]*header.ShopeeShop, 0)
	iter = session.Query(`SELECT data from proder.shopee_shop WHERE account_id=? LIMIT 1000`, id).Iter()
	b := make([]byte, 0)
	for iter.Scan(&b) {
		shop := &header.ShopeeShop{}
		if err := proto.Unmarshal(b, shop); err != nil {
			return nil, log.EData(err, b, log.M{"account_id": id})
		}
		shops = append(shops, shop)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"id": id})
	}

	iter = session.Query(`SELECT id, data FROM account.integrated_shipping WHERE account_id=? LIMIT ?`, id, 1000).Iter()
	ishippings := make([]*header.IntegratedShipping, 0)
	var shipid string
	var ishippingb []byte
	for iter.Scan(&shipid, &ishippingb) {
		ishipping := &header.IntegratedShipping{}
		proto.Unmarshal(ishippingb, ishipping)
		ishipping.Id = shipid
		ishipping.AccountId = id
		ishippings = append(ishippings, ishipping)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": id})
	}

	sps := []*header.ShippingPolicy{}
	data = []byte{}
	iter = session.Query(`SELECT data FROM account.shipping_policy WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		sp := header.ShippingPolicy{}
		proto.Unmarshal(data, &sp)
		sps = append(sps, &sp)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": id})
	}

	ccs := []*header.CancellationCode{}
	iter = session.Query(`SELECT data FROM account.cancellation_code WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		cc := header.CancellationCode{}
		proto.Unmarshal(data, &cc)
		ccs = append(ccs, &cc)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": id})
	}

	setting.CancellationCodes = ccs
	setting.Taxes = taxes
	setting.PaymentMethods = paymentmethods
	setting.Addresses = shopAddresses
	setting.Shippings = ishippings
	setting.ShippingPolicies = sps
	setting.ShopeeShops = shops
	setting.AccountId = id

	cache.Set("shop_setting."+id, setting)
	return setting, nil
}

func loadLangDB(accid, locale string, old *header.Lang, fallback bool) (*header.Lang, error) {
	waitUntilReady()

	lang := &header.Lang{}
	var message, lastmsg, updatedby, public, category string
	var updated int64
	var k string

	iter := session.Query(`SELECT k, message, public_state, last_message, updated, author, category FROM account.lang WHERE account_id=? AND locale=?`, accid, locale).Iter()
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
		return nil, log.ERetry(err, log.M{"account_id": accid})
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
	subscribe(accid+"_"+locale, "lang")

	lang, err := ListLocaleMessageDB(accid, locale)
	if err != nil {
		return nil, err
	}
	cache.Set("lang."+accid+"_"+locale, lang)
	return lang, nil
}

// see https://www.localeplanet.com/icu/
func GetLocale(accid, locale string) (*header.Lang, error) {
	if !header.LocaleM[locale] {
		return &header.Lang{}, nil
	}
	if value, found := cache.Get("lang." + accid + "_" + locale); found {
		if value == nil {
			return nil, nil
		}
		return value.(*header.Lang), nil
	}

	return listLocaleMessagesDB(accid, locale)
}

// TODO return proto clone of other methods
func GetAccount(accid string) (*pb.Account, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("account." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*pb.Account), nil
	}

	return getAccountDB(accid)
}

func MakeDefNotiSetting(accid, agid string) *header.NotiSetting {
	now := time.Now().UnixMilli()
	return &header.NotiSetting{
		AccountId: accid,
		AgentId:   agid,
		Web: &header.NotiSubscription{
			NewMessage:            true,
			UserCreated:           true,
			UserReturned:          true,
			CampaignUserConverted: true,
			UserOpenedEmail:       true,
			TicketUpdated:         now,
			TaskUpdated:           now,
		},
		Mobile: &header.NotiSubscription{NewMessage: true},
	}
}

func GetNotificationSetting(accid, agid string) (*header.NotiSetting, error) {
	subscribe(accid, "notification_setting")
	waitUntilReady()
	if value, found := cache.Get("notification_setting." + accid); found {
		if value == nil {
			return MakeDefNotiSetting(accid, agid), nil
		}
		list := value.([]*header.NotiSetting)
		for _, item := range list {
			if item.GetAgentId() == agid {
				return item, nil
			}
		}
		return MakeDefNotiSetting(accid, agid), nil
	}

	settings, err := getNotificationSettingDB(accid)
	if err != nil {
		return nil, err
	}
	for _, setting := range settings {
		if setting.GetAgentId() == agid {
			return setting, nil
		}
	}
	return MakeDefNotiSetting(accid, agid), nil
}

func GetSubscription(accid string) (*pm.Subscription, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("subscription." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*pm.Subscription), nil
	}

	return getSubDB(accid)
}

func listAgentsDB(accid string) (map[string]*pb.Agent, error) {
	subscribe(accid, "agent")
	waitUntilReady()
	listM := map[string]*pb.Agent{}

	res, err := accmgr.ListAgents(header.ToGrpcCtx(&compb.Context{Credential: &compb.Credential{AccountId: accid, Type: compb.Type_subiz}}), &header.Id{AccountId: accid})
	if err == nil && res != nil {
		for _, ag := range res.GetAgents() {
			if ag.GetState() != pb.Agent_deleted.String() {
				listM[ag.GetId()] = ag
			}
		}
		cache.Set("agent."+accid, listM)
		return listM, nil
	}

	var id, avatar_url, avatar_url_128, client_id, email, fullname, gender string
	var invited_by, jobtitle, lang, phone, state, typ, tz string
	var scopes []string
	var joined, passwordchanged, seen int64
	var dashboard_setting []byte
	var extension int64
	var modified int64
	iter := session.Query("SELECT id, avatar_url, avatar_url_128, client_id, dashboard_setting, email, fullname, gender, invited_by, job_title, joined, lang, modified, password_changed, phone, scopes, state, type, timezone, seen, extension FROM account.agents where account_id=?", accid).Iter()
	for iter.Scan(&id, &avatar_url, &avatar_url_128, &client_id, &dashboard_setting, &email, &fullname, &gender, &invited_by,
		&jobtitle, &joined, &lang, &modified, &passwordchanged, &phone, &scopes, &state, &typ, &tz, &seen, &extension) {
		ds := &pb.DashboardAgent{}
		proto.Unmarshal(dashboard_setting, ds)
		ag := &pb.Agent{
			AccountId:        conv.S(accid),
			Id:               conv.S(id),
			AvatarUrl:        conv.S(avatar_url),
			AvatarUrl_128:    conv.S(avatar_url_128),
			ClientId:         conv.S(client_id),
			DashboardSetting: ds,
			Email:            conv.S(email),
			Fullname:         conv.S(fullname),
			Gender:           conv.S(gender),
			InvitedBy:        conv.S(invited_by),
			JobTitle:         conv.S(jobtitle),
			Joined:           conv.PI64(int(joined)),
			Lang:             conv.S(lang),
			Modified:         conv.PI64(int(modified)),
			PasswordChanged:  conv.PI64(int(passwordchanged)),
			Phone:            conv.S(phone),
			Scopes:           scopes,
			State:            conv.S(state),
			Type:             conv.S(typ),
			Timezone:         conv.S(tz),
			Extension:        conv.PI64(int(extension)),
		}
		scopes = make([]string, 0)
		if ag.GetState() != pb.Agent_deleted.String() {
			listM[id] = ag
		}
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}

	cache.Set("agent."+accid, listM)
	return listM, nil
}

func listAttrDefsDB(accid string) (map[string]*header.AttributeDefinition, error) {
	subscribe(accid, "attribute_definition")
	defs := make(map[string]*header.AttributeDefinition, 0)
	iter := session.Query("SELECT data FROM user.attr_defs WHERE account_id=? LIMIT 1000", accid).Iter()
	var data []byte
	for iter.Scan(&data) {
		def := &header.AttributeDefinition{}
		if err := proto.Unmarshal(data, def); err != nil {
			return nil, log.ERetry(err, log.M{"account_id": accid})
		}
		defs[def.Key] = def
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}

	defaults := header.ListDefaultDefs()
	for _, a := range defaults {
		defs[a.Key] = a
	}

	cache.Set("attribute_definition."+accid, defs)
	return defs, nil
}

func getNotificationSettingDB(accid string) ([]*header.NotiSetting, error) {
	subscribe(accid, "notification_setting")
	waitUntilReady()
	agents, err := ListAgentM(accid)
	if err != nil {
		return nil, err
	}
	settings := []*header.NotiSetting{}
	for _, ag := range agents {
		agid := ag.GetId()
		setting := &header.NotiSetting{}
		data := []byte{}
		err := session.Query("SELECT data FROM notibox.setting WHERE accid=? AND agid=?", accid, agid).Scan(&data)
		if err != nil && err.Error() == gocql.ErrNotFound.Error() {
			setting = MakeDefNotiSetting(accid, agid)
			settings = append(settings, setting)
			continue
		}

		if err != nil {
			return nil, log.ERetry(err, log.M{"account_id": accid, "agent_id": agid})
		}

		proto.Unmarshal(data, setting)
		settings = append(settings, setting)
	}

	cache.Set("notification_setting."+accid, settings)
	return settings, nil
}

func listBotsDB(accid string) ([]*header.Bot, error) {
	subscribe(accid, "bot")
	waitUntilReady()
	iter := session.Query(`SELECT bot FROM bizbot.bots WHERE account_id=?`, accid).Iter()
	var botb []byte
	list := make([]*header.Bot, 0)
	for iter.Scan(&botb) {
		bot := &header.Bot{}
		proto.Unmarshal(botb, bot)
		bot.Action = nil
		if bot.GetState() != pb.Agent_deleted.String() {
			list = append(list, bot)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}

	cache.Set("bot."+accid, list)
	return list, nil
}

func listAIAgentsDB(accid string) (map[string]*header.AIAgent, error) {
	subscribe(accid, "ai_agent")
	waitUntilReady()
	iter := session.Query(`SELECT id, data FROM workflow.ai_agents WHERE accid=?`, accid).Iter()
	aiAgentM := map[string]*header.AIAgent{}
	var data []byte
	var id string
	for iter.Scan(&id, &data) {
		agent := &header.AIAgent{}
		proto.Unmarshal(data, agent)
		agent.Id = id
		aiAgentM[id] = agent
		data = []byte{}
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}

	cache.Set("ai_agent."+accid, aiAgentM)
	return aiAgentM, nil
}

func GetAgent(accid, agid string) (*pb.Agent, error) {
	agM, err := ListAgentM(accid)
	if err != nil {
		return nil, err
	}

	ag, has := agM[agid]
	if has {
		return ag, nil
	}

	if strings.HasPrefix(agid, "at") {
		return GetAIAgent(accid, agid)
	}

	if strings.HasPrefix(agid, "bb") {
		bot, err := GetBot(accid, agid)
		if err != nil {
			return nil, err
		}

		if bot != nil {
			return Bot2Agent(bot), nil
		}
	}

	return nil, nil
}

func AIAgent2Agent(bot *header.AIAgent) *pb.Agent {
	if bot == nil {
		return nil
	}

	active := "active"
	return &pb.Agent{
		AccountId:     &bot.AccountId,
		Id:            &bot.Id,
		State:         &active, // all bot are active as an agent, disabled -> hibernated
		AvatarUrl:     &bot.AvatarUrl,
		AvatarUrl_128: &bot.AvatarUrl,
		Fullname:      conv.S(bot.GetFullname()),
		Type:          conv.S(compb.Type_bot),
		Modified:      &bot.Updated,
		Joined:        &bot.Created,
		InvitedBy:     &bot.CreatedBy,
		Scopes:        []string{"agent"},
	}
}

func Bot2Agent(bot *header.Bot) *pb.Agent {
	active := "active"
	return &pb.Agent{
		AccountId:     &bot.AccountId,
		Id:            &bot.Id,
		State:         &active, // all bot are active as an agent, disabled -> hibernated
		AvatarUrl:     &bot.AvatarUrl,
		AvatarUrl_128: &bot.AvatarUrl_128,
		Fullname:      &bot.Fullname,
		Type:          conv.S(compb.Type_bot),
		Modified:      &bot.Updated,
		Joined:        &bot.Created,
		InvitedBy:     &bot.CreatedBy,
		Scopes:        []string{"agent"},
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
			for _, id := range group.GetAgentIds() {
				if ag, err := GetAgent(accid, id); err != nil {
					return nil, err
				} else {
					out = append(out, ag)
				}
			}
			return out, nil
		}
	}
	return nil, nil
}

func ListAgentM(accid string) (map[string]*pb.Agent, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("agent." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*pb.Agent), nil
	}

	return listAgentsDB(accid)
}

func ListGroups(accid string) ([]*header.AgentGroup, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("agent_group." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*header.AgentGroup), nil
	}
	return listGroupsDB(accid)
}

func listGroupsDB(accid string) ([]*header.AgentGroup, error) {
	subscribe(accid, "agent_group")
	waitUntilReady()
	var arr = make([]*header.AgentGroup, 0)
	iter := session.Query("SELECT id, data FROM account.agent_groups WHERE account_id=? LIMIT 500", accid).Iter()
	var id string
	data := make([]byte, 0)
	for iter.Scan(&id, &data) {
		group := &header.AgentGroup{}
		if err := proto.Unmarshal(data, group); err != nil {
			return nil, log.EData(err, data, log.M{"account_id": accid, "group_id": id})
		}
		group.AccountId = accid
		group.Id = id
		data = make([]byte, 0)
		arr = append(arr, group)
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}
	cache.Set("agent_group."+accid, arr)
	return arr, nil
}

func ListOnlineAgents(accid string) ([]*pb.Presence, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("presence." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.Presence), nil
	}
	return listPresencesDB(accid)
}

func listPresencesDB(accid string) ([]*pb.Presence, error) {
	waitUntilReady()
	subscribe(accid, "presence")

	pres, err := accmgr.ListAgentOnlines(context.Background(), &header.ListAgentOnlineRequest{AccountId: accid})
	if err != nil {
		return nil, err
	}
	presences := pres.GetPresences()
	cache.Set("presence."+accid, presences)
	return presences, nil
}

func ListActiveAccountIds() ([]string, error) {
	waitUntilReady()
	res, err := accmgr.ListActiveAccountIds(context.Background(), &header.Id{})
	if err != nil {
		return nil, err
	}
	return res.GetIds(), nil
}

func GetAIAgent(accid, agid string) (*pb.Agent, error) {
	aiags, err := ListAIAgents(accid)
	if err != nil {
		return nil, err
	}

	return AIAgent2Agent(aiags[agid]), nil
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
	if value, found := cache.Get("bot." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*header.Bot), nil
	}
	return listBotsDB(accid)
}

func ListAIAgents(accid string) (map[string]*header.AIAgent, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("ai_agent." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.AIAgent), nil
	}
	return listAIAgentsDB(accid)
}

func listPipelineDB(accid string) ([]*header.Pipeline, error) {
	waitUntilReady()
	subscribe(accid, "pipeline")
	iter := session.Query(`SELECT id, pipeline FROM apiece.pipelines WHERE account_id=? LIMIT 100`, accid).Iter()
	pipelines := make([]*header.Pipeline, 0)
	var dbid string
	var pipelineb []byte
	for iter.Scan(&dbid, &pipelineb) {
		pipeline := &header.Pipeline{}
		proto.Unmarshal(pipelineb, pipeline)
		pipeline.AccountId = accid
		pipeline.Id = dbid
		pipelines = append(pipelines, pipeline)
	}
	err := iter.Close()
	if err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}
	cache.Set("pipeline."+accid, pipelines)
	return pipelines, nil
}

func ListPipelines(accid string) ([]*header.Pipeline, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("pipeline." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*header.Pipeline), nil
	}
	return listPipelineDB(accid)
}

func SignKey(accid, issuer, typ, keytype string, objects []string) (string, error) {
	waitUntilReady()
	key := randomID("SK", 28)
	err := session.Query(`INSERT INTO account.signed_key(account_id, issuer, type, objects, key_type, key, created) VALUES(?,?,?,?,?,?,?)`, accid, issuer, typ, objects, keytype, key, time.Now().UnixMilli()).Exec()
	if err != nil {
		return "", log.ERetry(err, log.M{"account_id": accid, "issuer": issuer, "type": typ, "keytype": keytype})
	}

	return key, nil
}

func LookupSignedKey(key string) (string, string, string, string, []string, error) {
	waitUntilReady()
	var accid, issuer, typ, keytype string
	objects := make([]string, 0)
	err := session.Query(`SELECT account_id, issuer, type, key_type, objects FROM account.signed_key WHERE key=?`, key).Scan(&accid, &issuer, &typ, &keytype, &objects)
	if err != nil {
		return "", "", "", "", nil, log.ERetry(err, log.M{"key": key})
	}

	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return "", "", "", "", nil, nil
	}

	return accid, issuer, typ, keytype, objects, nil
}

func ListDefs(accid string) (map[string]*header.AttributeDefinition, error) {
	waitUntilReady()
	if value, found := cache.Get("attribute_definition." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.AttributeDefinition), nil
	}
	return listAttrDefsDB(accid)
}

func GetShopSetting(accid string) (*header.ShopSetting, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("shop_setting." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*header.ShopSetting), nil
	}

	return getShopSettingDb(accid)
}

// account currency /order currency  (E.g: order currency: VND, acc currency: USD, => currency_rate = 1/20k = 0.00005)
func ConvertToFPV(accid string, price float32, order_cur string) (int64, float32, error) {
	acc, err := GetAccount(accid)
	if err != nil {
		return 0, 0, err
	}
	setting, err := GetShopSetting(accid)
	if err != nil {
		return 0, 0, err
	}

	defcur := strings.TrimSpace(acc.GetCurrency())
	if defcur == "" && order_cur != "" {
		return 0, 0, log.Error3(nil, log.M{
			"_message": map[string]string{
				"En_US": "Invalid base currency. You must specify base currency setting for your account",
				"Vi_VN": "Tiền tệ cơ sở không hợp lệ. Bạn cần thiết lập tiền tệ cơ sở cho tài khoản trước",
			},
			"account_id": accid,
		}, log.E_internal)
	}

	if order_cur == "" {
		order_cur = defcur
	}

	if defcur == order_cur {
		return int64(price * 1000000), 1, nil
	}

	// order_cur and defcur must not be empty and be difference
	// first, calculate fpv
	for _, cur := range setting.GetOtherCurrencies() {
		if cur.GetCode() != order_cur {
			continue
		}

		if cur.GetRate() <= 0 {
			return 0, 0, log.Error3(nil, log.M{
				"_message": map[string]string{
					"En_US": fmt.Sprintf("Wrong currency rate (%f). Please contact Support for support", cur.GetRate()),
					"Vi_VN": fmt.Sprintf("Tỉ giá tiền không hợp lệ (%f). Vui lòng liên hệ Subiz để được hỗ trợ", cur.GetRate()),
				},
				"account_id": accid,
				"rate":       cur.GetRate(),
			}, log.E_internal)
		}
		return int64(price * cur.GetRate() * 1000000), cur.GetRate(), nil
	}
	return 0, 0, log.Error3(nil, log.M{
		"_message": map[string]string{
			"En_US": fmt.Sprintf("Unsupported currency (%s)", order_cur),
			"Vi_VN": fmt.Sprintf("Tiền tệ (%s) không được hỗ trợ", order_cur),
		},
		"account_id": accid,
		"currency":   order_cur,
	}, log.E_internal)
}

// letterRunes (read-only) contains all runes which can be used in an ID
var letterRunes = []rune("abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ123456789")

func randomID(sign string, randomfactor int) string {
	var sb strings.Builder
	sb.WriteString(sign)
	for i := 0; i < randomfactor; i++ {
		sb.WriteRune(letterRunes[rand.Intn(len(letterRunes))])
	}
	return sb.String()
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

func NewID(accid, scope string) int64 {
	waitUntilReady()
	for attempt := 0; attempt < 100; attempt++ {
		id, err := accmgr.NewID(context.Background(), &header.Id{AccountId: accid, Id: scope})
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		idint, _ := strconv.ParseInt(id.Id, 10, 0)
		return idint
	}
	return -1
}

func NewID2(accid, scope string) int64 {
	scope = ascii.Convert(scope)
	waitUntilReady()
	for attempt := 0; attempt < 100; attempt++ {
		id, err := registryClient.NewID2(context.Background(), &header.Id{AccountId: accid, Id: scope})
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		idint, _ := strconv.ParseInt(id.Id, 10, 0)
		return idint
	}
	return -1
}

func GetLastID(accid, scope string) int64 {
	scope = ascii.Convert(scope)
	waitUntilReady()
	for attempt := 0; attempt < 100; attempt++ {
		id, err := registryClient.GetLastID(context.Background(), &header.Id{AccountId: accid, Id: scope})
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		idint, _ := strconv.ParseInt(id.Id, 10, 0)
		return idint
	}
	return -1
}

func GetAttrAsStringWithDateFormat(user *header.User, key, dateformat string) string {
	accid := user.AccountId
	if accid == "" {
		return ""
	}

	var foundAttr *header.Attribute
	for _, attr := range user.Attributes {
		if attr.GetKey() == key {
			foundAttr = attr
			break
		}
	}

	if foundAttr == nil {
		return ""
	}

	defM, _ := ListDefs(accid)
	if defM == nil {
		return ""
	}

	def := defM[key]
	if def == nil {
		return ""
	}

	if def.Type == "text" {
		return foundAttr.Text
	}

	if def.Type == "number" {
		b, _ := json.Marshal(foundAttr.GetNumber())
		return string(b)
	}

	if def.Type == "boolean" {
		if foundAttr.GetBoolean() {
			return "true"
		}
		return "false"
	}

	if def.Type == "datetime" {
		// timezone
		timezone := header.GetTextAttr(user, "timezone")
		if timezone == "" {
			// fallback to account timeonze
			// to get timezone
			acc, _ := GetAccount(accid)
			timezone = acc.GetTimezone()
		}

		t, err := time.Parse(time.RFC3339, foundAttr.GetDatetime())
		if err != nil {
			t = time.Now()
		}

		tzhour, tzmin, _ := business_hours.SplitTzOffset(timezone)
		tInTz := t.UTC().Add(time.Hour*time.Duration(tzhour) + time.Minute*time.Duration(tzmin))
		return tInTz.Format(dateformat)
	}
	return ""
}

func GetAttrAsString(user *header.User, key string) string {
	var foundAttr *header.Attribute
	for _, attr := range user.Attributes {
		if attr.GetKey() == key {
			foundAttr = attr
			break
		}
	}

	if foundAttr == nil {
		return ""
	}

	defM, _ := ListDefs(user.AccountId)
	if defM == nil {
		return ""
	}

	def := defM[key]
	if def == nil {
		return ""
	}

	if def.Type == "text" {
		return foundAttr.Text
	}

	if def.Type == "number" {
		b, _ := json.Marshal(foundAttr.GetNumber())
		return string(b)
	}

	if def.Type == "boolean" {
		if foundAttr.GetBoolean() {
			return "true"
		}
		return "false"
	}

	if def.Type == "datetime" {
		return foundAttr.GetDatetime()
	}
	return ""
}

func GetCreditUsage(accid, creditId string, key string) (*header.CreditUsage, error) {
	ctx := header.ToGrpcCtx(&compb.Context{Credential: &compb.Credential{AccountId: accid, Type: compb.Type_subiz}})
	res, err := creditmgr.GetTotalCreditSpend(ctx, &header.Id{AccountId: accid, Id: creditId + "." + key})
	if err != nil {
		return nil, err
	}
	return res.GetCreditUsage(), nil
}

func TrySpendCredit(accid, creditId string, price float64) error {
	waitUntilReady()
	if accid == "" || creditId == "" {
		return nil // alway allow
	}
	var credit *header.Credit
	if val, has := creditCache.Get(accid + "." + creditId); has {
		credit = val.(*header.Credit)
	} else {
		ctx := header.ToGrpcCtx(&compb.Context{Credential: &compb.Credential{AccountId: accid, Type: compb.Type_subiz}})
		credits, err := creditmgr.ListCredits(ctx, &header.Id{AccountId: accid, Id: creditId})
		if err != nil {
			return err
		}
		for _, freshCredit := range credits.GetCredits() {
			if freshCredit.GetId() == creditId {
				credit = freshCredit
			}
			creditCache.Set(accid+"."+creditId, freshCredit)
		}
	}

	if credit == nil {
		return log.EMissing(creditId, "credit", log.M{"accid": accid, "credit_id": creditId})
	}

	// quick estimated
	if credit.GetFpvBalance()+credit.GetFpvCreditLimit() > int64(price*1_000_000)*2 {
		return nil
	}

	// must ask
	_, err := creditmgr.TrySpendCredit(context.Background(), &header.CreditSpendEntry{
		AccountId:    accid,
		CreditId:     creditId,
		Quantity:     1,
		FpvUnitPrice: int64(price * 1_000_000),
	})
	return err
}

func RecordCredit(ctx *compb.Context, accid, creditId, itemType, itemId string, quantity int64, price float64, data *header.CreditEntryData) {
	if accid == "" || creditId == "" {
		return // alway allow
	}

	byagid := ""
	if ctx.GetCredential().GetType().String() == "agent" {
		byagid = ctx.GetCredential().GetIssuer()
	}

	serviceid := ctx.GetCredential().GetIssuer()
	kafka.Publish("kafka-1:9092", "credit-spend-log", &header.CreditSpendEntry{
		AccountId:    accid,
		CreditId:     creditId,
		Id:           idgen.NewPaymentLogID(),
		Item:         itemType,
		ItemId:       itemId,
		ServiceId:    serviceid,
		Created:      time.Now().UnixMilli(),
		Quantity:     quantity,
		FpvUnitPrice: int64(price * 1_000_000),
		Data:         data,
		ByAgentId:    byagid,
	})
}

func Notify(accid, topic string) {
	waitUntilReady()
	numpubsub.Fire(context.Background(), &header.PsMessage{
		AccountId: accid,
		Event: &header.Event{
			AccountId: accid,
			Type:      topic,
			Created:   time.Now().UnixMilli(),
		},
		Topics: []string{topic + "." + accid},
	})
}

func pollLoop() {
	connId := idgen.NewPollingConnId("0", "", randstr.Hex(8))
	for {
		time.Sleep(1 * time.Second)
		topics := []string{}
		subscribeTopicLock.Lock()
		for topic := range subscribeTopics {
			topics = append(topics, topic)
		}
		subscribeTopicLock.Unlock()

		out, err := numpubsub.Poll(context.Background(), &header.RealtimeSubscription{Events: topics, ConnectionId: connId})
		if err != nil {
			fmt.Println("ERR", connId, err)
			time.Sleep(30 * time.Second)
			continue
		}
		for _, event := range out.GetEvents() {
			accid := event.GetAccountId()
			if accid == "" {
				continue
			}
			cache.Delete(event.GetType() + "." + accid)
		}
	}
}

var emptyM = map[string]bool{}
var agentScopeCache = gocache.New(30 * time.Second)

func joinMap(a, b map[string]bool) {
	for k, v := range b {
		if v {
			a[k] = v
		}
	}
}

func MustBeSuperAdmin(cred *compb.Credential) error {
	if cred.GetAccountId() != "acpxkgumifuoofoosble" {
		return EACCESS_DENY
	}

	if cred.GetType() == compb.Type_subiz {
		return nil
	}

	agent, err := GetAgent(cred.GetAccountId(), cred.GetIssuer())
	if err != nil {
		return err
	}

	if agent.GetState() != "active" {
		return EACCESS_DENY
	}
	for _, scope := range agent.GetScopes() { // agent's account-wide scope
		if header.ScopeM[scope] != nil && header.ScopeM[scope]["accmgr:update"] {
			return nil
		}
	}
	return EACCESS_DENY
}

func AccessFeature(objectType header.ObjectType, action header.ObjectAction, cred *compb.Credential) error {
	if cred.GetType() == compb.Type_subiz || cred.GetType() == compb.Type_workflow || cred.GetType() == compb.Type_connector {
		return nil
	}

	if action == "" {
		return nil
	}

	accid := cred.GetAccountId()
	if accid == "" || cred.GetIssuer() == "" {
		return EACCESS_DENY
	}

	agent, err := GetAgent(accid, cred.GetIssuer())
	if err != nil {
		return err
	}

	permM := map[string]bool{}
	if agent.GetState() == "active" {
		for _, scope := range agent.GetScopes() { // agent's account-wide scope
			joinMap(permM, header.ScopeM[scope])
		}
	}
	// ticket:read ticket:write
	if permM[string(objectType)+":"+string(action)+":none"] {
		return EACCESS_DENY
	}

	if permM[string(objectType)+":"+string(action)] || permM[string(objectType)+":"+string(action)+":all"] {
		return nil
	}

	return EACCESS_DENY
}

func CheckPerm(objectType header.ObjectType, action header.ObjectAction, accid, issuer, issuertype string, isOwned, isAssigned bool, resourceGroups ...header.IResourceGroup) error {
	if issuertype == "system" || issuertype == "subiz" || issuertype == "connector" {
		return nil
	}

	if action == "" {
		return nil
	}
	for _, resourceGroup := range resourceGroups {
		pM, err := GetAgentPerm(accid, issuer, resourceGroup)
		if err != nil {
			return err
		}
		if CheckAgentPerm(objectType, action, pM, isOwned, isAssigned) {
			return nil
		}
	}

	if len(resourceGroups) == 0 && accid != "" {
		agent, err := GetAgent(accid, issuer)
		if err != nil {
			return err
		}
		if agent.GetState() == "active" {
			for _, scope := range agent.GetScopes() { // agent's account-wide scope
				if CheckAgentPerm(objectType, action, header.ScopeM[scope], isOwned, isAssigned) {
					return nil
				}
			}
		}
	}
	return EACCESS_DENY
}

func CheckAgentPerm(objectType header.ObjectType, action header.ObjectAction, permM map[string]bool, isOwned, isAssigned bool) bool {
	if permM[string(objectType)+":"+string(action)+":none"] {
		return false // 	// explicitly prevent this action
	}

	if permM[string(objectType)+":"+string(action)] || permM[string(objectType)+":"+string(action)+":all"] {
		return true
	}

	if isOwned {
		// ticket:read:own ticket:write:own
		if permM[string(objectType)+":"+string(action)+":own"] {
			return true
		}
	}

	if !isAssigned {
		// ticket:read:own ticket:write:own
		if permM[string(objectType)+":"+string(action)+":unassigned"] {
			return true
		}
	}
	return false
}

func GetAgentPerm(accid, agid string, resourceGroup header.IResourceGroup) (map[string]bool, error) {
	if accid == "" || agid == "" {
		return emptyM, nil
	}

	resourceGroupId := ""
	if resourceGroup != nil {
		resourceGroupId = resourceGroup.GetId()
	}
	if value, found := agentScopeCache.Get(accid + "_" + agid + "_" + resourceGroupId); found && value != nil {
		return value.(map[string]bool), nil
	}

	agent, err := GetAgent(accid, agid)
	if err != nil {
		return nil, err
	}

	if agent == nil || agent.GetState() != "active" {
		agentScopeCache.Set(accid+"_"+agid+"_"+resourceGroupId, emptyM)
		return emptyM, nil
	}

	permM := map[string]bool{}
	agentScopes := agent.GetScopes() // agent's account-wide scope
	if resourceGroup == nil {
		for _, scope := range agentScopes {
			joinMap(permM, header.ScopeM[scope])
		}
		agentScopeCache.Set(accid+"_"+agid+"_"+resourceGroupId, permM)
		return permM, nil
	}

	var myGroup map[string]bool
	for _, mem := range resourceGroup.GetPermissions() {
		if mem.GetMemberId() == agid {
			// agent is directly set role in this group ->
			// we dont want to use this setting instead of merging role
			// so we must reset the perm and break right after
			permM = map[string]bool{} //
			for _, scope := range mem.Scopes {
				joinMap(permM, header.ScopeM[scope])
			}
			break
		}

		if mem.GetMemberId() == "*" {
			for _, scope := range mem.Scopes {
				joinMap(permM, header.ScopeM[scope])
			}
		}

		if !strings.HasPrefix(mem.GetMemberId(), "gr") {
			continue
		}
		if myGroup == nil {
			myGroup = map[string]bool{}
			groups, err := ListGroups(accid)
			if err != nil {
				return nil, err
			}
			for _, group := range groups {
				if header.ContainString(group.GetAgentIds(), agid) {
					myGroup[group.GetId()] = true
				}
			}
		}

		if !myGroup[mem.GetMemberId()] {
			continue
		}
		for _, scope := range mem.Scopes {
			joinMap(permM, header.ScopeM[scope])
		}
	}
	for _, scope := range agentScopes {
		joinMap(permM, header.ScopeM[scope])
	}
	agentScopeCache.Set(accid+"_"+agid+"_"+resourceGroupId, permM)
	return permM, nil
}

func ListBlacklistIPs(accid string) (map[string]*header.BlacklistIP, error) {
	waitUntilReady()

	// cache exists
	if value, found := cache.Get("blacklist_ip." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.BlacklistIP), nil
	}
	return listBlacklistIPsDB(accid)
}

// ListBlacklistIPs returns all blacklist IPs for an account
// it may updates cache if needed
func listBlacklistIPsDB(accid string) (map[string]*header.BlacklistIP, error) {
	subscribe(accid, "blacklist_ip")
	waitUntilReady()

	ips := map[string]*header.BlacklistIP{}
	iter := session.Query(`SELECT ip, "by", created, num_blocked, last_blocked, expired_at FROM api.wlips WHERE account_id=? LIMIT 1000`, accid).Iter()
	var ip, by string
	var created, num_blocked, expired, last_blocked int64
	for iter.Scan(&ip, &by, &created, &num_blocked, &last_blocked, &expired) {
		if expired == 0 {
			expired = clock.UnixMili(created) + 86400*90*1000 // 90 days after created
		}

		if last_blocked+expired < time.Now().UnixMilli() {
			session.Query(`DELETE FROM api.wlips WHERE account_id=? AND ip=?`, accid, ip).Exec()
			continue
		}
		ips[ip] = &header.BlacklistIP{AccountId: accid, Ip: ip, By: by, Created: clock.UnixMili(created), NumBlocked: num_blocked, ExpiredAt: expired, LastBlocked: last_blocked}
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}
	cache.Set("blacklist_ip."+accid, ips)
	return ips, nil
}

func ListBannedUsers(accid string) (map[string]*header.BannedUser, error) {
	waitUntilReady()

	// cache exists
	if value, found := cache.Get("banned_user." + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.BannedUser), nil
	}
	return listBannedUserDB(accid)
}

func listBannedUserDB(accid string) (map[string]*header.BannedUser, error) {
	subscribe(accid, "banned_user")

	users := map[string]*header.BannedUser{}
	iter := session.Query(`SELECT user_id, "by", created FROM api.wlusers WHERE account_id=? LIMIT 10000`, accid).Iter()
	var userid, by string
	var created int64
	for iter.Scan(&userid, &by, &created) {
		users[userid] = &header.BannedUser{AccountId: accid, UserId: userid, By: by, Created: created}
	}
	if err := iter.Close(); err != nil {
		return nil, log.ERetry(err, log.M{"account_id": accid})
	}
	cache.Set("banned_user."+accid, users)
	return users, nil
}

const COUNTERSHARD = 4

func IncreaseCounter(timeseries []string, count int64, createdMs int64) {
	tss := [COUNTERSHARD][]string{}
	for _, ts := range timeseries {
		shard := int(crc32.ChecksumIEEE([]byte(ts))) % COUNTERSHARD
		tss[shard] = append(tss[shard], ts)
	}

	for i, ts := range tss {
		if len(ts) > 0 {
			kafka.Publish("kafka-1:9092", "counter-"+strconv.Itoa(i), &header.CounterDataPoint{TimeSeries: ts, Created: createdMs, Count: count})
		}
	}
}

func ReportCounter(timeseries string, fromSec int64, _range string, limit int64) ([]int64, error) {
	shard := int(crc32.ChecksumIEEE([]byte(timeseries))) % COUNTERSHARD
	out, err := GetCounterClient(shard).Report(context.Background(), &header.CounterReportRequest{
		TimeSeries: timeseries,
		FromSec:    fromSec,
		Range:      _range,
		Limit:      limit,
	})
	if err != nil {
		return nil, err
	}
	return out.GetCounts(), nil
}

func IncreaseCounter2(timeserieCountM map[string]int64, id string, createdMs int64) {
	tss := [COUNTERSHARD]map[string]int64{}
	for ts, count := range timeserieCountM {
		shard := int(crc32.ChecksumIEEE([]byte(ts))) % COUNTERSHARD
		if tss[shard] == nil {
			tss[shard] = map[string]int64{}
		}
		tss[shard][ts] = count
	}

	for i, tsM := range tss {
		if len(tsM) > 0 {
			tses := []string{}
			counts := []int64{}
			for ts, count := range tsM {
				tses = append(tses, ts)
				counts = append(counts, count)
			}
			kafka.Publish("kafka-1:9092", "counter-"+strconv.Itoa(i), &header.CounterDataPoint{TimeSeries: tses, Created: createdMs, Counts: counts, Id: id})
		}
	}
}

func CountCounter2(timeseries string, fromSec int64, _range string, limit int64) (int64, error) {
	shard := int(crc32.ChecksumIEEE([]byte(timeseries))) % COUNTERSHARD
	out, err := GetCounterClient(shard).Count(context.Background(), &header.CounterReportRequest{
		TimeSeries: timeseries,
		FromSec:    fromSec,
		Range:      _range,
		Limit:      limit,
	})
	if err != nil {
		return 0, err
	}
	if len(out.GetCounts()) < 1 {
		return 0, nil
	}
	return out.GetCounts()[0], nil
}

var _counterClient [COUNTERSHARD]header.CounterClient

func GetCounterClient(shard int) header.CounterClient {
	if _counterClient[shard] != nil {
		return _counterClient[shard]
	}
	conn := header.DialGrpc(fmt.Sprintf("counter-%d.counter:12306", shard))
	client := header.NewCounterClient(conn)
	_counterClient[shard] = client
	return client
}

func subscribe(accid, topic string) {
	waitUntilReady()
	subscribeTopicLock.Lock()
	subscribeTopics[topic+"."+accid] = true
	subscribeTopicLock.Unlock()
}

func GetAccPar(accid string, N int) string {
	if accid == "acpxkgumifuoofoosble" || accid == "acqsulrowbxiugvginhw" {
		return "stg"
	}
	return strconv.Itoa(int(crc32.ChecksumIEEE([]byte(accid))) % N)
}
