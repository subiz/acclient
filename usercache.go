package acclient

import (
	"context"
	"time"

	"github.com/subiz/header"
	"github.com/subiz/log"
	"github.com/thanhpk/breaker"
)

var cb = breaker.New()

var _usercache header.UserCacheClient
var _usercachestg header.UserCacheClient

func getUserCacheClient(accid string) header.UserCacheClient {
	if header.IsStagging(accid) {
		if _usercachestg != nil {
			return _usercachestg
		}
		_usercachestg = header.NewUserCacheClient(header.DialGrpc("diskcache:14954"))
		return _usercachestg
	} else {
		if _usercache != nil {
			return _usercache
		}
		_usercache = header.NewUserCacheClient(header.DialGrpc("diskcache:14955"))
		return _usercache
	}
}

func UpdateUser(ctx context.Context, user *header.User) error {
	if user == nil {
		return nil
	}

	if ctx == nil {
		ctx = context.Background()
	}
	accid := user.GetAccountId()
	client := getUserCacheClient(accid)
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = client.UpdateUser(ctx, user); err != nil {
				log.Err(accid, err)
				log.Track(ctx, "user_cache_down", "account_id", accid, "user_id", user.GetId(), "err", err)
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
	client := getUserCacheClient(accid)
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = client.AddUsersToSegment(ctx, &header.SegmentUsersRequest{
				AccountId: accid,
				SegmentId: segid,
				UserIds:   userids,
			}); err != nil {
				log.Err(accid, err)
				log.Track(ctx, "user_cache_down", "account_id", accid, "seg_id", segid, "user_ids", userids, "err", err)
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
	client := getUserCacheClient(accid)
	return cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if _, err = client.RemoveUsersFromSegment(context.Background(), &header.SegmentUsersRequest{
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
	client := getUserCacheClient(accid)
	var resp *header.Response
	err := cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if resp, err = client.ListLeads(ctx, view); err != nil {
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

func CountLeads(ctx context.Context, view *header.UserView) (*header.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	accid := view.GetAccountId()
	client := getUserCacheClient(accid)
	var resp *header.Response
	err := cb.Execute(func() error {
		var err error
		for range 4 { // retry 4 times
			if resp, err = client.CountLeads(ctx, view); err != nil {
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
