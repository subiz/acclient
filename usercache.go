package acclient

import (
	"time"
	"os"
	"strings"
	"context"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"github.com/thanhpk/breaker"
)

var cb = breaker.New()

var usercache header.UserCacheClient

func getUserCacheClient() header.UserCacheClient {
	if usercache != nil {
		return usercache
	}

	hostname, _ := os.Hostname()
	if strings.HasSuffix(hostname, "-stg-0") {
		usercache = header.NewUserCacheClient(header.DialGrpc("diskcache:14954"))
	} else {
		usercache = header.NewUserCacheClient(header.DialGrpc("diskcache:14955"))
	}
	return usercache
}

func UpdateUser(ctx context.Context, user *header.User) error {
	if ctx == nil {
		ctx = context.Background()
	}
	usercache = getUserCacheClient()
	accid := user.GetAccountId()
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = usercache.UpdateUser(ctx, user); err != nil {
				log.Err(accid, err)
				log.Track(nil, "user_cache_down", "account_id", accid, "user_id", user.GetId(), "err", err)
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
		return err
	})
}

func AddUsersToSegment(ctx context.Context, accid, segid string, userids []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	usercache = getUserCacheClient()
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = usercache.AddUsersToSegment(context.Background(), &header.SegmentUsersRequest{
				AccountId: accid,
				SegmentId: segid,
				UserIds:   userids,
			}); err != nil {
				log.Err(accid, err)
				log.Track(nil, "user_cache_down", "account_id", accid, "seg_id", segid, "user_ids", userids, "err", err)
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
		return err
	})
}

func RemoveUserFromSegment(ctx context.Context, accid, segid string, userids []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	usercache = getUserCacheClient()
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = usercache.RemoveUsersFromSegment(context.Background(), &header.SegmentUsersRequest{
				AccountId: accid,
				SegmentId: segid,
				UserIds:   userids,
			}); err != nil {
				log.Err(accid, err)
				log.Track(nil, "user_cache_down", "account_id", accid, "seg_id", segid, "user_ids", userids, "err", err)
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
		return err
	})
}

func ListLeads(ctx context.Context, view *header.UserView) (*header.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	accid := view.GetAccountId()
	usercache = getUserCacheClient()
	var resp *header.Response
	err := cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if resp, err = usercache.ListLeads(ctx, view); err != nil {
				log.Err(accid, err)
				log.Track(nil, "user_cache_down", "account_id", accid, "view", view, "err", err)
				time.Sleep(5 * time.Second)
				continue
			}
			return nil
		}
		return err
	})
	return resp, err
}
