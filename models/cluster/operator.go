// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"errors"

	"forgejo.org/models/db"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/timeutil"
)

// Operator grants the cluster-operator permission to an account: a site
// administrator listed here may change the cluster from the administration
// (kitsunium ADR 0152 D19, R7-27). It is distinct from site administration:
// a site administrator who is not listed sees the cluster and cannot change
// it.
type Operator struct {
	UserID    int64 `xorm:"pk"`
	GrantedBy int64 `xorm:"NOT NULL DEFAULT 0"`
	// OIDCSubject is the identity at [cluster] STEP_UP_OIDC_SOURCE this
	// operator re-authenticates as. It is pinned on the grant, or from a
	// node with `forgejo cluster operator pin`, never from the account's own
	// settings: deleting or replacing an external login link changes nothing.
	OIDCSubject string             `xorm:"'oidc_subject' VARCHAR(255) NOT NULL DEFAULT ''"`
	CreatedUnix timeutil.TimeStamp `xorm:"created"`
}

// TableName names the table explicitly.
func (Operator) TableName() string { return "cluster_operator" }

func init() {
	db.RegisterModel(new(Operator))
}

// IsOperator reports whether an account holds the permission.
func IsOperator(ctx context.Context, userID int64) (bool, error) {
	return db.GetEngine(ctx).Exist(&Operator{UserID: userID})
}

// ListOperators returns every grant, oldest first.
func ListOperators(ctx context.Context) ([]*Operator, error) {
	var out []*Operator
	return out, db.GetEngine(ctx).OrderBy("created_unix, user_id").Find(&out)
}

// CountOperators counts the grants.
func CountOperators(ctx context.Context) (int64, error) {
	return db.GetEngine(ctx).Count(new(Operator))
}

// GetOperator returns an account's grant, if any.
func GetOperator(ctx context.Context, userID int64) (*Operator, bool, error) {
	o := &Operator{}
	has, err := db.GetEngine(ctx).ID(userID).Get(o)
	return o, has, err
}

// AddOperator grants the permission with the OpenID subject pinned for its
// step-up (empty when none applies); granting it twice is not an error and
// keeps the first grant.
func AddOperator(ctx context.Context, userID, grantedBy int64, oidcSubject string) error {
	if ok, err := IsOperator(ctx, userID); err != nil || ok {
		return err
	}
	_, err := db.GetEngine(ctx).Insert(&Operator{UserID: userID, GrantedBy: grantedBy, OIDCSubject: oidcSubject})
	return err
}

// PinOperatorSubject sets the OpenID subject an operator steps up as.
func PinOperatorSubject(ctx context.Context, userID int64, subject string) error {
	n, err := db.GetEngine(ctx).ID(userID).Cols("oidc_subject").Update(&Operator{OIDCSubject: subject})
	if err == nil && n == 0 {
		if ok, _ := IsOperator(ctx, userID); !ok {
			return errors.New("the account is not a cluster operator")
		}
	}
	return err
}

// ErrLastOperator refuses to withdraw the last grant.
var ErrLastOperator = errors.New("the last cluster operator cannot be withdrawn")

// RemoveOperator withdraws the permission.
func RemoveOperator(ctx context.Context, userID int64) error {
	_, err := db.GetEngine(ctx).Delete(&Operator{UserID: userID})
	return err
}

// RemoveOperatorUnlessLast withdraws the permission unless it is the last
// grant, atomically: the grants are locked (FOR UPDATE on PostgreSQL and
// MySQL; SQLite lets one writer commit and fails the other), so two
// operators withdrawing each other at once cannot both succeed and leave
// nobody.
func RemoveOperatorUnlessLast(ctx context.Context, userID int64) error {
	return db.WithTx(ctx, func(ctx context.Context) error {
		var ids []int64
		sess := db.GetEngine(ctx).Table("cluster_operator").Cols("user_id")
		if setting.Database.Type.IsMySQL() || setting.Database.Type.IsPostgreSQL() {
			sess = sess.ForUpdate()
		}
		if err := sess.Find(&ids); err != nil {
			return err
		}
		held := false
		for _, id := range ids {
			held = held || id == userID
		}
		if !held {
			return nil
		}
		if len(ids) <= 1 {
			return ErrLastOperator
		}
		_, err := db.GetEngine(ctx).Delete(&Operator{UserID: userID})
		return err
	})
}
