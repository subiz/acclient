package acclient

import (
	"context"
	"time"

	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/kafka"
	"github.com/subiz/log"
	"google.golang.org/protobuf/proto"
)

// Flow
// an integration represents a channel unit, for examples: a Facebook Fanpage, a Zalo OA,
//   an email address, a phone number (call-ccenter), a zalopersonal account, a tiktok channel, ...
// an integration have name, avatar picture and a state
// for example:
// { "name": "Faster than time", connector_type: "facebook", logo_url: "https://examples.com/logo.jpg", state: "activated" }
//
// Problem: we need to keep integration in connector and conversation-service in sync
// Solution: here is the protocol
//
// [connector view]
// 0. connector listen for sync event from conversation by calling acclient.OnIntegrationUpdate
// 1. when user authorized the fanpage, connector know, connector collect the fanpage name, avatar and other informations,
//    note that the default state is activated
// 2. connector persists integration to the database, also, call acclient.ConnectorUpdateIntegration to notify conversation
//    service
// 3. [optional] conversation-service will try to notify all of its connectors
// 4. since the integration update if initialized by the connector itself, the connector should ignore that event from
//    the conversation-service (or better, conversation-service should not notify in the first place, hence step 3 is
//    optional).
//
// [conversation-service view]
//
//
// connector decides:
//   which account an integration belong to
//
// [state transition]
// these are all allowed transition of state:
// activated -> deactivated, by convo when out of money
// activated -> deactivated, by connector since user authorize fanpage for another account
// activated -> failed, by connector when the token is expired
// activated -> deleted, by convo when user delete the integration
// deactivated -> deleted (by convo when user delete the integration
// activated -> deleted,by convo when user delete the integration
// deactivated -> activated, by connector, when user authorize
// failed -> activated, by connector, when user authorize
// deleted -> activated, by connector, when user authorize
// failed -> deleted, by connector, when user authorize

var convoclient header.ConversationMgrClient

func GetConvoClient() header.ConversationMgrClient {
	if convoclient != nil {
		return convoclient
	}

	defer header.KLock("convolock_2482350")()
	if convoclient != nil {
		return convoclient
	}

	conn := header.DialGrpc("convo-0.convo:18021", header.WithShardRedirect())
	convoclient = header.NewConversationMgrClient(conn)
	return convoclient
}

// zaloperson-0, or fabikon-4
func OnIntegrationUpdate(connectorType, serviceid string, accLookup func(*header.Integration) string, cb func(*header.Integration)) error {
	_, err := kafka.Consume("kafka-1:9092", serviceid, "integration-updated", func(_ int32, _ int64, data []byte, _ string) {
		inte := &header.Integration{}
		proto.Unmarshal(data, inte)
		if inte.GetAccountId() == "" {
			return
		}

		if inte.GetConnectorType() != connectorType {
			return
		}

		if inte.GetCtx().GetCredential().GetType() == cpb.Type_connector { // prevent loop
			return
		}

		correctAccid := accLookup(inte)
		if inte.GetAccountId() != correctAccid {
			return
		}
		cb(inte)
	})
	return err
}

// service: fabikon/zaloperson
func ConnectorUpdateIntegration(service string, inte *header.Integration) error {
	accid := inte.GetAccountId()
	ctx := &cpb.Context{
		AccountId: accid,
		Credential: &cpb.Credential{
			Scopes:   []string{"all"},
			Issuer:   service,
			Type:     cpb.Type_connector, // prevent loop
			ClientId: service,
		},
	}
	if _, err := GetConvoClient().UpsertIntegration(header.ToGrpcCtx(ctx), inte); err != nil {
		log.Track(context.Background(), "re-integrate-error", "account_id", accid, "service", service, "inte", inte)
		return err
	}
	return nil
}

// service: zaloperson
func NotifyOutdatedIntegration(service, accid, inteid string) {
	ctx := &cpb.Context{
		AccountId: accid,
		Credential: &cpb.Credential{
			Scopes:   []string{"all"},
			Issuer:   service,
			Type:     cpb.Type_connector, // prevent loop
			ClientId: service,
		},
	}
	inte, _ := GetConvoClient().GetIntegration(header.ToGrpcCtx(ctx), &header.Id{AccountId: accid, Id: inteid})
	if inte != nil {
		if inte.GetState() == "deactivated" || inte.GetState() == "deleted" {
			return // nothing todo
		}
		inte.State = "deactivated"
		inte.TokenStatus = "failed"
		inte.TokenStatusUpdated = time.Now().UnixMilli()
		inte.ErrorCode = "unlinked"
		if _, err := GetConvoClient().UpsertIntegration(header.ToGrpcCtx(ctx), inte); err != nil {
			log.Track(context.Background(), "re-integrate-error", "account_id", accid, "service", service)
		}
	}
}
