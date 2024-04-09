package acclient

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/goutils/business_hours"
	"github.com/subiz/goutils/conv"
	"github.com/subiz/header"
	pb "github.com/subiz/header/account"
	compb "github.com/subiz/header/common"
	n5pb "github.com/subiz/header/noti5"
	pm "github.com/subiz/header/payment"
	"github.com/subiz/idgen"
	"github.com/subiz/kafka"
	"github.com/subiz/log"
	gocache "github.com/thanhpk/go-cache"
	"github.com/thanhpk/randstr"
	"google.golang.org/protobuf/proto"
)

const (
	tblLocale = "lang"
	tblAgents = "agents"
	tblGroups = "groups"
)

var (
	readyLock = &sync.Mutex{}

	ready bool

	session        *gocql.Session
	accmgr         header.AccountMgrClient
	creditmgr      header.CreditMgrClient
	registryClient header.NumberRegistryClient

	numpubsub header.PubsubClient

	cache      = gocache.New(5 * time.Minute)
	hash_cache *gocache.Cache

	creditCache = gocache.New(60 * time.Second) // accid+"."+creditid
)

func _init() {
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
	var businesshourb, invoice_infob, leadsetting, userattributesetting []byte
	var supportedlocales []string
	var address, city, country, dateformat, facebook, lang, locale, logo_url, logo_url_128, name, ownerid, phone, referrer_from, region, state, timezone, twitter, url string
	var created, modified int64
	var zipcode int32
	var currency string
	var currency_locked bool

	err := session.Query("SELECT address, business_hours,city, country, created,date_format, facebook, lang, lead_setting, user_attribute_setting, locale, logo_url, logo_url_128, modified, name, owner_id, phone, referrer_from, region, state, supported_locales, timezone, twitter, url, zip_code, currency, currency_locked, invoice_info FROM account.accounts WHERE id=?", id).Scan(&address, &businesshourb, &city, &country, &created, &dateformat, &facebook, &lang, &leadsetting, &userattributesetting, &locale, &logo_url, &logo_url_128, &modified, &name, &ownerid, &phone, &referrer_from, &region, &state, &supportedlocales, &timezone, &twitter, &url, &zipcode, &currency, &currency_locked, &invoice_infob)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.Set("ACC_"+id, nil)
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, log.EServer(err, log.M{"id": id})
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
		Facebook:             &facebook,
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
		Twitter:              &twitter,
		Url:                  &url,
		ZipCode:              &zipcode,
		Currency:             &currency,
		CurrencyLocked:       &currency_locked,
		UserAttributeSetting: uas,
		InvoiceInfo:          ii,
	}
	cache.Set("ACC_"+id, acc)

	var limitb []byte
	var autocharge bool
	var plan, pmmethod, promo, referralby string
	var subcreated, churned, ended, started, fpv_unlimited_agent_price int64
	var billingcyclemonth, next_billing_cycle_month uint32
	var credit float32
	var customerb []byte

	err = session.Query("SELECT \"limit\", auto_charge, billing_cycle_month, created, credit, customer, ended, churned, next_billing_cycle_month, plan, primary_payment_method, promotion_code, referral_by, started, fpv_unlimited_agent_price FROM account.subs WHERE account_id=?", id).Scan(
		&limitb, &autocharge, &billingcyclemonth, &subcreated, &credit, &customerb, &ended, &churned, &next_billing_cycle_month, &plan, &pmmethod, &promo, &referralby, &started, &fpv_unlimited_agent_price)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.Set("SUB_"+id, nil)
		return acc, nil, nil
	}
	if err != nil {
		return nil, nil, log.EServer(err, log.M{"account_id": id})
	}

	limit := &compb.Limit{}
	proto.Unmarshal(limitb, limit)

	customer := &pm.Customer{}
	proto.Unmarshal(customerb, customer)
	sub := &pm.Subscription{
		AccountId:              &id,
		BillingCycleMonth:      &billingcyclemonth,
		Created:                &subcreated,
		Credit:                 &credit,
		Limit:                  limit,
		Customer:               customer,
		Ended:                  &ended,
		Churned:                &churned,
		Plan:                   &plan,
		NextBillingCycleMonth:  &next_billing_cycle_month,
		PrimaryPaymentMethod:   &pmmethod,
		PromotionCode:          &promo,
		ReferralBy:             &referralby,
		Started:                &started,
		FpvUnlimitedAgentPrice: &fpv_unlimited_agent_price,
	}
	cache.Set("SUB_"+id, sub)
	return acc, sub, nil
}

func getShopSettingDb(id string) (*header.ShopSetting, error) {
	waitUntilReady()
	var data = []byte{}
	err := session.Query("SELECT data FROM account.shop_setting WHERE account_id=?", id).Scan(&data)
	setting := &header.ShopSetting{}

	if err != nil && err.Error() != gocql.ErrNotFound.Error() {
		return nil, log.EServer(err, log.M{"id": id})
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
		return nil, log.EServer(err, log.M{"id": id})
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
		return nil, log.EServer(err, log.M{"id": id})
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
		return nil, log.EServer(err, log.M{"id": id})
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
		return nil, log.EServer(err, log.M{"id": id})
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
		return nil, log.EServer(err, log.M{"account_id": id})
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
		return nil, log.EServer(err, log.M{"account_id": id})
	}

	ccs := []*header.CancellationCode{}
	iter = session.Query(`SELECT data FROM account.cancellation_code WHERE account_id=?`, id).Iter()
	for iter.Scan(&data) {
		cc := header.CancellationCode{}
		proto.Unmarshal(data, &cc)
		ccs = append(ccs, &cc)
	}
	if err := iter.Close(); err != nil {
		return nil, log.EServer(err, log.M{"account_id": id})
	}

	setting.CancellationCodes = ccs
	setting.Taxes = taxes
	setting.PaymentMethods = paymentmethods
	setting.Addresses = shopAddresses
	setting.Shippings = ishippings
	setting.ShippingPolicies = sps
	setting.ShopeeShops = shops
	setting.AccountId = id

	cache.Set("SHOPSETTING_"+id, setting)
	return setting, nil
}

func loadLangDB(accid, locale string, old *header.Lang, fallback bool) (*header.Lang, error) {
	waitUntilReady()

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
		return nil, log.EServer(err, log.M{"account_id": accid})
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
	cache.Set("LANG_"+accid+"_"+locale, lang)
	return lang, nil
}

// see https://www.localeplanet.com/icu/
func GetLocale(accid, locale string) (*header.Lang, error) {
	if !header.LocaleM[locale] {
		return &header.Lang{}, nil
	}
	if value, found := cache.Get("LANG_" + accid + "_" + locale); found {
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
	if value, found := cache.Get("ACC_" + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*pb.Account), nil
	}

	acc, _, err := getAccountDB(accid)
	return acc, err
}

func makeDefNotiSetting(accid, agid string) *n5pb.Setting {
	now := time.Now().UnixMilli()
	return &n5pb.Setting{
		AccountId: &accid,
		AgentId:   &agid,
		Web: &n5pb.Subscription{
			NewMessage:            conv.B(true),
			UserCreated:           conv.B(true),
			UserReturned:          conv.B(true),
			CampaignUserConverted: conv.B(true),
			UserOpenedEmail:       conv.B(true),
			TaskUpdated:           &now,
		},
		Mobile: &n5pb.Subscription{NewMessage: conv.B(true)},
	}
}
func GetNotificationSetting(accid, agid string) (*n5pb.Setting, error) {
	waitUntilReady()
	if value, found := cache.Get("N5Setting_" + accid); found {
		if value == nil {
			return makeDefNotiSetting(accid, agid), nil
		}
		list := value.([]*n5pb.Setting)
		for _, item := range list {
			if item.GetAgentId() == agid {
				return item, nil
			}
		}
		return makeDefNotiSetting(accid, agid), nil
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
	return makeDefNotiSetting(accid, agid), nil
}

func GetSubscription(accid string) (*pm.Subscription, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("SUB_" + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*pm.Subscription), nil
	}

	_, sub, err := getAccountDB(accid)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func listAgentsDB(accid string) ([]*pb.Agent, error) {
	waitUntilReady()
	acc, err := GetAccount(accid)
	if err != nil {
		return nil, err
	}
	ownerId := acc.GetOwnerId()

	var arr = make([]*pb.Agent, 0)

	var id, avatar_url, avatar_url_128, client_id, email, encrypted_password, fullname, gender string
	var invited_by, jobtitle, lang, phone, state, typ, tz string
	var issupervisor bool
	var scopes []string
	var joined, passwordchanged, seen int64
	var dashboard_setting []byte
	var extension int64
	var modified int64

	iter := session.Query("SELECT id, avatar_url, avatar_url_128, client_id, dashboard_setting, email, encrypted_password, fullname, gender, invited_by, is_supervisor, job_title, joined, lang, modified, password_changed, phone, scopes, state, type, timezone, seen, extension FROM account.agents where account_id=?", accid).Iter()
	for iter.Scan(&id, &avatar_url, &avatar_url_128, &client_id, &dashboard_setting, &email, &encrypted_password, &fullname, &gender, &invited_by,
		&issupervisor, &jobtitle, &joined, &lang, &modified, &passwordchanged, &phone, &scopes, &state, &typ, &tz, &seen, &extension) {
		ds := &pb.DashboardAgent{}
		proto.Unmarshal(dashboard_setting, ds)
		ag := &pb.Agent{
			AccountId:         conv.S(accid),
			Id:                conv.S(id),
			AvatarUrl:         conv.S(avatar_url),
			AvatarUrl_128:     conv.S(avatar_url_128),
			ClientId:          conv.S(client_id),
			DashboardSetting:  ds,
			Email:             conv.S(email),
			EncryptedPassword: conv.S(encrypted_password),
			Fullname:          conv.S(fullname),
			Gender:            conv.S(gender),
			InvitedBy:         conv.S(invited_by),
			IsOwner:           conv.B(id == ownerId),
			IsSupervisor:      conv.B(issupervisor),
			JobTitle:          conv.S(jobtitle),
			Joined:            conv.PI64(int(joined)),
			Lang:              conv.S(lang),
			Modified:          conv.PI64(int(modified)),
			PasswordChanged:   conv.PI64(int(passwordchanged)),
			Phone:             conv.S(phone),
			Scopes:            scopes,
			State:             conv.S(state),
			Type:              conv.S(typ),
			Timezone:          conv.S(tz),
			Extension:         conv.PI64(int(extension)),
		}
		scopes = make([]string, 0)
		arr = append(arr, ag)
	}

	if err := iter.Close(); err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid})
	}

	list := make([]*pb.Agent, 0)
	for _, a := range arr {
		if a.GetState() != pb.Agent_deleted.String() {
			a.EncryptedPassword = nil
			list = append(list, a)
		}
	}

	cache.Set("AG_"+accid, list)
	return list, nil
}

func listAttrDefsDB(accid string) (map[string]*header.AttributeDefinition, error) {
	defs := make(map[string]*header.AttributeDefinition, 0)
	iter := session.Query("SELECT data FROM user.attr_defs WHERE account_id=? LIMIT 1000", accid).Iter()
	var data []byte
	for iter.Scan(&data) {
		def := &header.AttributeDefinition{}
		if err := proto.Unmarshal(data, def); err != nil {
			return nil, log.EServer(err, log.M{"account_id": accid})
		}
		defs[def.Key] = def
	}
	if err := iter.Close(); err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid})
	}

	defaults := ListDefaultDefs()
	for _, a := range defaults {
		defs[a.Key] = a
	}

	cache.Set("ATTRDEF_"+accid, defs)
	return defs, nil
}

func getNotificationSettingDB(accid string) ([]*n5pb.Setting, error) {
	waitUntilReady()
	agents, err := ListAgents(accid)
	if err != nil {
		return nil, err
	}
	settings := []*n5pb.Setting{}
	for _, ag := range agents {
		agid := ag.GetId()
		dnd, em, mobile, web := make([]byte, 0), make([]byte, 0), make([]byte, 0), make([]byte, 0)
		var instant_mute_until, updated int64
		err := session.Query(`SELECT do_not_disturb, email, instant_mute_until, mobile, updated, web FROM noti5.notisettings WHERE account_id=? AND agent_id=?`, accid, agid).Scan(&dnd, &em, &instant_mute_until, &mobile, &updated, &web)
		if err != nil && err.Error() == gocql.ErrNotFound.Error() {
			now := time.Now().UnixMilli()
			// default setting
			setting := &n5pb.Setting{
				AccountId: &accid,
				AgentId:   conv.S(agid),
				Web: &n5pb.Subscription{
					NewMessage:            conv.B(true),
					UserCreated:           conv.B(true),
					UserReturned:          conv.B(true),
					CampaignUserConverted: conv.B(true),
					UserOpenedEmail:       conv.B(true),
					TaskUpdated:           &now,
				},
				Mobile: &n5pb.Subscription{NewMessage: conv.B(true)},
			}
			settings = append(settings, setting)
			continue
		}

		if err != nil {
			return nil, log.EServer(err, log.M{"account_id": accid, "agent_id": agid})
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
			AgentId:          conv.S(agid),
			DoNotDisturb:     dnds,
			InstantMuteUntil: &instant_mute_until,
			Updated:          &updated,
			Web:              webs,
			Mobile:           mobiles,
			Email:            email,
		}
		settings = append(settings, setting)
	}

	cache.Set("N5Setting_"+accid, settings)
	return settings, nil
}

func listBotsDB(accid string) ([]*header.Bot, error) {
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
		return nil, log.EServer(err, log.M{"account_id": accid})
	}

	cache.Set("BOT_"+accid, list)
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

func Bot2Agent(bot *header.Bot) *pb.Agent {
	return &pb.Agent{
		AccountId:     &bot.AccountId,
		Id:            &bot.Id,
		State:         &bot.State,
		AvatarUrl:     &bot.AvatarUrl,
		AvatarUrl_128: &bot.AvatarUrl_128,
		Fullname:      &bot.Fullname,
		Type:          conv.S(compb.Type_bot),
		Modified:      &bot.Updated,
		Joined:        &bot.Created,
		InvitedBy:     &bot.CreatedBy,
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

func ListAgents(accid string) ([]*pb.Agent, error) {
	waitUntilReady()

	// cache exists
	if value, found := cache.Get("AG_" + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.Agent), nil
	}
	return listAgentsDB(accid)
}

func ListGroups(accid string) ([]*header.AgentGroup, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("GR_" + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*header.AgentGroup), nil
	}
	return listGroupsDB(accid)
}

func listGroupsDB(accid string) ([]*header.AgentGroup, error) {
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
		return nil, log.EServer(err, log.M{"account_id": accid})
	}
	cache.Set("GR_"+accid, arr)
	return arr, nil
}

func ListPresences(accid string) ([]*pb.Presence, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("PS_" + accid); found {
		if value == nil {
			return nil, nil
		}
		return value.([]*pb.Presence), nil
	}
	return listPresencesDB(accid)
}

func listPresencesDB(accid string) ([]*pb.Presence, error) {
	waitUntilReady()

	pres, err := accmgr.ListAgentPresences(context.Background(), &header.Id{Id: accid})
	if err != nil {
		return nil, err
	}
	presences := pres.GetPresences()
	cache.SetWithExpire("PS_"+accid, presences, 10*time.Second)
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
		if value == nil {
			return nil, nil
		}
		return value.([]*header.Bot), nil
	}
	return listBotsDB(accid)
}

func listPipelineDB(accid string) ([]*header.Pipeline, error) {
	waitUntilReady()
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
		return nil, log.EServer(err, log.M{"account_id": accid})
	}
	cache.Set("PIPELINE_"+accid, pipelines)
	return pipelines, nil
}

func ListPipelines(accid string) ([]*header.Pipeline, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("PIPELINE_" + accid); found {
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
	err := session.Query(`INSERT INTO account.signed_key(account_id, issuer, type, objects, key_type, key, created) VALUES(?,?,?,?,?,?,?)`, accid, issuer, typ, objects, keytype, key, time.Now().UnixNano()/1e6).Exec()
	if err != nil {
		return "", log.EServer(err, log.M{"account_id": accid, "issuer": issuer, "type": typ, "keytype": keytype})
	}

	return key, nil
}

func LookupSignedKey(key string) (string, string, string, string, []string, error) {
	waitUntilReady()
	var accid, issuer, typ, keytype string
	objects := make([]string, 0)
	err := session.Query(`SELECT account_id, issuer, type, key_type, objects FROM account.signed_key WHERE key=?`, key).Scan(&accid, &issuer, &typ, &keytype, &objects)
	if err != nil {
		return "", "", "", "", nil, log.EServer(err, log.M{"key": key})
	}

	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return "", "", "", "", nil, nil
	}

	return accid, issuer, typ, keytype, objects, nil
}

func ListDefs(accid string) (map[string]*header.AttributeDefinition, error) {
	waitUntilReady()
	if value, found := cache.Get("ATTRDEF_" + accid); found {
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
	if value, found := cache.Get("SHOPSETTING_" + accid); found {
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
		return 0, 0, log.Error(nil, log.M{
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
			return 0, 0, log.Error(nil, log.M{
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
	return 0, 0, log.Error(nil, log.M{
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

func TrySpendCredit(accid, creditId, service, serviceId, itemType, itemId string, quantity int64, price float64) error {
	waitUntilReady()
	if accid == "" || creditId == "" {
		return nil // alway allow
	}
	var credit *header.Credit
	if val, has := creditCache.Get(accid + "." + creditId); has {
		credit = val.(*header.Credit)
	} else {
		credits, err := creditmgr.ListCredits(context.Background(), &header.Id{AccountId: accid, Id: creditId})
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
		return log.EMissing(creditId, "credit", log.M{"accid": accid})
	}

	// quick estimated
	if credit.GetFpvBalance()+credit.GetFpvCreditLimit() > quantity*int64(price*1_000_000)*2 {
		return nil
	}

	// must ask
	_, err := creditmgr.TrySpendCredit(context.Background(), &header.CreditSpendEntry{
		AccountId:    accid,
		CreditId:     creditId,
		Id:           idgen.NewPaymentLogID(),
		Service:      service,
		ServiceId:    serviceId,
		Item:         itemType,
		ItemId:       itemId,
		Created:      time.Now().UnixMilli(),
		Quantity:     quantity,
		FpvUnitPrice: int64(price * 1_000_000),
	})
	return err
}

func RecordCredit(accid, creditId, service, serviceId, itemType, itemId string, quantity int64, price float64, data *header.CreditSendEntryData) {
	if accid == "" || creditId == "" {
		return // alway allow
	}
	kafka.Publish("credit-spend-log", &header.CreditSpendEntry{
		AccountId:    accid,
		CreditId:     creditId,
		Id:           idgen.NewPaymentLogID(),
		Service:      service,
		ServiceId:    serviceId,
		Item:         itemType,
		ItemId:       itemId,
		Created:      time.Now().UnixMilli(),
		Quantity:     quantity,
		FpvUnitPrice: int64(price * 1_000_000),
		Data:         data,
	})
}

func Notify(accid, id string, topic string) {
	waitUntilReady()
	numpubsub.Fire(context.Background(), &header.PsMessage{AccountId: accid, Event: &header.Event{AccountId: accid, Id: id, Created: time.Now().UnixMilli(), Type: topic}, Topics: []string{topic}})
}

func pollLoop() {
	conn := header.DialGrpc("numreg-0.numreg:8665")
	client := header.NewPubsubClient(conn)

	topics := []string{"account_updated", "lang_updated", "shop_setting_updated", "agent_updated", "agent_group_updated", "bot_updated", "attribute_definition_updated", "notisetting_updated", "pipeline_updated"}
	connId := idgen.NewPollingConnId("0", "", randstr.Hex(8))
	for {
		time.Sleep(2 * time.Second)
		out, err := client.Poll(context.Background(), &header.RealtimeSubscription{Events: topics, ConnectionId: connId})
		if err != nil {
			fmt.Println("ERR", err)
			time.Sleep(30 * time.Second)
		}
		for _, event := range out.GetEvents() {
			accid := event.GetAccountId()
			if accid == "" {
				continue
			}
			if event.GetType() == "account_updated" {
				cache.Delete("ACC_" + accid)
				cache.Delete("SUB_" + accid)
			}
			if event.GetType() == "shop_setting_updated" {
				cache.Delete("SHOPSETTING_" + accid)
			}

			if event.GetType() == "lang_updated" {
				rawCache := cache.Items()
				for k := range rawCache {
					if strings.HasPrefix(k, "LANG_"+accid+"_") {
						cache.Delete(k)
					}
				}
			}
			if event.GetType() == "agent_updated" {
				cache.Delete("AG_" + accid)
			}
			if event.GetType() == "agent_group_updated" {
				cache.Delete("GR_" + accid)
			}
			if event.GetType() == "bot_updated" {
				cache.Delete("BOT_" + accid)
			}
			if event.GetType() == "attribute_definition_updated" {
				cache.Delete("ATTRDEF_" + accid)
			}
			if event.GetType() == "notisetting_updated" {
				cache.Delete("N5Setting_" + accid)
			}
			if event.GetType() == "pipeline_updated" {
				cache.Delete("PIPELINE_" + accid)
			}
		}
	}
}
