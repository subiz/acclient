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
	"google.golang.org/protobuf/proto"
)

const (
	tblLocale       = "lang"
	tblAgents       = "agents"
	tblGroups       = "groups"
	tblSubscription = "subs"
	tblPresence     = "convo.presence"
	tblBots         = "bizbot.bots"
)

var (
	readyLock = &sync.Mutex{}
	keyLock   = &sync.Mutex{}

	keys30   = map[string]bool{}
	keys60   = map[string]bool{}
	keysLang = map[string]bool{}

	ready bool

	session        *gocql.Session
	accmgr         header.AccountMgrClient
	creditmgr      header.CreditMgrClient
	registryClient header.NumberRegistryClient

	compactM      = map[string]int{}
	compactLock   = &sync.Mutex{}
	uncompactM    = map[int]string{}
	uncompactLock = &sync.Mutex{}

	cache      = gocache.New(2 * time.Minute)
	hash_cache *gocache.Cache
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

	conn := header.DialGrpc("account-0.account:10283", header.WithShardRedirect())
	accmgr = header.NewAccountMgrClient(conn)
	creditmgr = header.NewCreditMgrClient(conn)

	connRegistry := header.DialGrpc("registry-0.registry:8665", header.WithShardRedirect())
	registryClient = header.NewNumberRegistryClient(connRegistry)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			accids := []string{}

			keyLock.Lock()
			for accid := range keys60 {
				accids = append(accids, accid)
			}
			keys60 = map[string]bool{}
			keyLock.Unlock()

			for _, accid := range accids {
				getAccountDB(accid)
				listAgentsDB(accid)
				listGroupsDB(accid)
				getShopSettingDb(accid)
				listPipelineDB(accid)
			}
			time.Sleep(50 * time.Second)
		}
	}()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			accids := []string{}

			keyLock.Lock()
			for accid := range keys30 {
				accids = append(accids, accid)
			}
			keys30 = map[string]bool{}
			keyLock.Unlock()

			for _, accid := range accids {
				getNotificationSettingDB(accid)
				listPresencesDB(accid)
			}
			time.Sleep(10 * time.Second)
		}
	}()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			accids := []string{}

			keyLock.Lock()
			for accid := range keysLang {
				accids = append(accids, accid)
			}
			keysLang = map[string]bool{}
			keyLock.Unlock()

			for _, ks := range accids {
				ks := strings.Split(ks, ";")
				listLocaleMessagesDB(ks[0], ks[1])
			}
			time.Sleep(10 * time.Second)
		}
	}()

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
	var businesshourb, feature, force_feature, leadsetting, monitorsetting []byte
	var supportedlocales []string
	var address, city, country, dateformat, facebook, lang, locale, logo_url, name, ownerid, phone, referrer_from, region, state, tax_id, timezone, twitter, url string
	var confirmed, created, lasttokenrequested, modified int64
	var zipcode int32
	var currency string
	var currency_locked bool

	err := session.Query("SELECT address, business_hours,city,confirmed, country, created,date_format, facebook, feature, force_feature, lang, last_token_requested, lead_setting, locale, logo_url, modified, name, owner_id, phone, referrer_from, region, state, supported_locales, tax_id, timezone, twitter, url, webpage_monitor_setting, zip_code, currency, currency_locked FROM account.accounts WHERE id=?", id).Scan(&address, &businesshourb, &city, &confirmed, &country, &created, &dateformat, &facebook, &feature, &force_feature, &lang, &lasttokenrequested, &leadsetting, &locale, &logo_url, &modified, &name, &ownerid, &phone, &referrer_from, &region, &state, &supportedlocales, &tax_id, &timezone, &twitter, &url, &monitorsetting, &zipcode, &currency, &currency_locked)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithExpire("ACC_"+id, nil, 30*time.Second)
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, log.EServer(err, log.M{"id": id})
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
	cache.SetWithExpire("ACC_"+id, acc, 60*time.Second)

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
		cache.SetWithExpire("SUB_"+id, nil, 30*time.Second)
		return acc, nil, nil
	}
	if err != nil {
		return nil, nil, log.EServer(err, log.M{"account_id": id})
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
	cache.SetWithExpire("SUB_"+id, sub, 30*time.Second)
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

	cache.SetWithExpire("SHOPSETTING_"+id, setting, 60*time.Second)
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
	cache.SetWithExpire("LANG_"+accid+"_"+locale, lang, 30*time.Second)
	return lang, nil
}

// see https://www.localeplanet.com/icu/
func GetLocale(accid, locale string) (*header.Lang, error) {
	if !header.LocaleM[locale] {
		return &header.Lang{}, nil
	}
	if value, found := cache.Get("LANG_" + accid + "_" + locale); found {
		keyLock.Lock()
		keysLang[accid+";"+locale] = true // trigger reading from db for future read
		keyLock.Unlock()
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
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()

		if value == nil {
			return nil, nil
		}
		return value.(*pb.Account), nil
	}

	acc, _, err := getAccountDB(accid)
	return acc, err
}

func GetNotificationSetting(accid, agid string) (*n5pb.Setting, error) {
	waitUntilReady()
	keyLock.Lock()
	keys30[accid] = true
	keyLock.Unlock()

	if value, found := cache.Get("N5Setting_" + accid + "_" + agid); found {
		if value == nil {
			return nil, nil
		}
		return value.(*n5pb.Setting), nil
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
	}, nil
}

func GetSubscription(accid string) (*pm.Subscription, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("SUB_" + accid); found {
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()
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
	var arr = make([]*pb.Agent, 0)

	var id, avatar_url, client_id, country_code, email, encrypted_password, fullname, gender string
	var invited_by, jobtitle, lang, phone, state, typ, tz string
	var isowner, issupervisor bool
	var scopes []string
	var joined, lasttokenrequested, passwordchanged, seen int64
	var dashboard_setting []byte
	var extension int64
	var modified int64

	iter := session.Query("SELECT id, avatar_url, client_id, country_code, dashboard_setting, email, encrypted_password, fullname, gender, invited_by, is_owner, is_supervisor, job_title, joined, lang, last_token_requested, modified, password_changed, phone, scopes, state, type, timezone, seen, extension FROM account.agents where account_id=?", accid).Iter()
	for iter.Scan(&id, &avatar_url, &client_id, &country_code, &dashboard_setting, &email, &encrypted_password, &fullname, &gender, &invited_by,
		&isowner, &issupervisor, &jobtitle, &joined, &lang, &lasttokenrequested, &modified, &passwordchanged, &phone, &scopes, &state, &typ, &tz, &seen, &extension) {
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
			Extension:          conv.PI64(int(extension)),
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

	cache.SetWithExpire("AG_"+accid, list, 30*time.Second)
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

	cache.SetWithExpire("ATTRDEF_"+accid, defs, 60*time.Second)
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
			cache.SetWithExpire("N5Setting_"+accid+"_"+agid, setting, 120*time.Second)
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
		cache.SetWithExpire("N5Setting_"+accid+"_"+agid, setting, 120*time.Second)
	}
	return settings, nil
}

func listBotsDB(accid string) ([]*header.Bot, error) {
	waitUntilReady()
	iter := session.Query(`SELECT bot FROM `+tblBots+` WHERE account_id=?`, accid).Iter()
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

	cache.SetWithExpire("BOT_"+accid, list, 60*time.Second)
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

	var agents []*pb.Agent
	// cache exists
	if value, found := cache.Get("AG_" + accid); found {
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()
		if value == nil {
			return nil, nil
		}
		agents = value.([]*pb.Agent)
	} else {
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()
		var err error
		agents, err = listAgentsDB(accid)
		if err != nil {
			return nil, err
		}
	}
	return agents, nil
}

func ListGroups(accid string) ([]*header.AgentGroup, error) {
	waitUntilReady()
	// cache exists
	if value, found := cache.Get("GR_" + accid); found {
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()
		if value == nil {
			return nil, nil
		}
		return value.([]*header.AgentGroup), nil
	}

	keyLock.Lock()
	keys60[accid] = true
	keyLock.Unlock()
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
	cache.SetWithExpire("GR_"+accid, arr, 30*time.Second)
	return arr, nil
}

func ListPresences(accid string) ([]*pb.Presence, error) {
	waitUntilReady()

	keyLock.Lock()
	keys30[accid] = true
	keyLock.Unlock()

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
	presences := make([]*pb.Presence, 0)

	iter := session.Query(`SELECT user_id, ip, pinged, ua, last_seen_convo_id FROM `+tblPresence+` WHERE account_id=? LIMIT 1000`, accid).Iter()
	uid, ip, ua, last_convoid := "", "", "", ""
	pinged := int64(0)
	for iter.Scan(&uid, &ip, &pinged, &ua, &last_convoid) {
		presences = append(presences, &pb.Presence{
			AccountId:       conv.S(accid),
			UserId:          conv.S(uid),
			Ip:              conv.S(ip),
			Pinged:          conv.PI64(int(pinged)),
			Ua:              conv.S(ua),
			LastSeenConvoId: conv.S(last_convoid),
		})
	}
	if err := iter.Close(); err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid})
	}

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
	cache.SetWithExpire("PIPELINE_"+accid, pipelines, 30*time.Second)
	return pipelines, nil
}

func ListPipelines(accid string) ([]*header.Pipeline, error) {
	keyLock.Lock()
	keys60[accid] = true
	keyLock.Unlock()

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
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()
		if value == nil {
			return nil, nil
		}
		return value.(map[string]*header.AttributeDefinition), nil
	}

	keyLock.Lock()
	keys60[accid] = true
	keyLock.Unlock()
	return listAttrDefsDB(accid)
}

func GetShopSetting(accid string) (*header.ShopSetting, error) {
	waitUntilReady()
	// cache hit
	if value, found := cache.Get("SHOPSETTING_" + accid); found {
		keyLock.Lock()
		keys60[accid] = true
		keyLock.Unlock()

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
		if err == nil {
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

func CheckRecordCredit(accid, creditId, service, serviceId, itemType, itemId string, quantity int64, price float32) (bool, error) {
	res, err := creditmgr.TrySpendCredit(context.Background(), &header.CreditSpendEntry{
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
	if err != nil {
		return false, err
	}

	return res.GetAllow(), nil
}

func TryRecordCredit(accid, creditId, service, serviceId, itemType, itemId string, quantity int64, price float64, data *header.CreditSendEntryData) error {
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
	if err != nil {
		return err
	}
	return nil
}

func RecordCredit(accid, creditId, service, serviceId, itemType, itemId string, quantity int64, price float64, data *header.CreditSendEntryData) {
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

func getCompactStr(str string) (int, error) {
	waitUntilReady()
	compactLock.Lock()
	number, exist := compactM[str]
	compactLock.Unlock()
	if exist {
		fmt.Println("Compact: get from cache")
		return number, nil
	}
	number, err := getCompactStrDB(str)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func getCompactStrDB(str string) (int, error) {
	waitUntilReady()
	var num int
	err := session.Query(`SELECT num FROM account.compact_str WHERE str=?`, str).Scan(&num)

	if err != nil {
		return 0, err
	}
	fmt.Println("Compact: get from db")
	uncompactLock.Lock()
	uncompactM[num] = str
	compactM[str] = num
	uncompactLock.Unlock()

	return num, nil
}

func CompactStr(ctx context.Context, str string) (int, error) {
	waitUntilReady()
	number, err := getCompactStr(str)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {

		numberOut, err := registryClient.Compact(ctx, &header.String{
			Str: str,
		})
		if err != nil {
			return 0, log.EServer(err, log.M{"str": str})
		}
		number = int(numberOut.GetNumber())
		return number, nil
	}

	if err != nil {
		return 0, log.EServer(err, log.M{"str": str})
	}

	return number, nil
}

func getUncompactNumDB(num int) (string, error) {
	waitUntilReady()
	var str string

	err := session.Query(`SELECT str FROM account.uncompact_num WHERE num=?`, num).Scan(&str)

	if err != nil {
		return "", err
	}
	uncompactLock.Lock()
	uncompactM[num] = str
	compactM[str] = num
	uncompactLock.Unlock()

	fmt.Println("Uncompact: get from db")
	return str, nil
}

func UncompactNum(num int) (string, error) {
	waitUntilReady()
	uncompactLock.Lock()
	str, exist := uncompactM[num]
	uncompactLock.Unlock()
	if exist {
		fmt.Println("Uncompact: get from cache")
		return str, nil
	}
	str, err := getUncompactNumDB(num)

	if err != nil {
		return "", err
	}
	return str, nil
}
