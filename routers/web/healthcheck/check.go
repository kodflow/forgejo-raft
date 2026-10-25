// Copyright 2022 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package healthcheck

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"forgejo.org/models/db"
	"forgejo.org/modules/cache"
	"forgejo.org/modules/json"
	"forgejo.org/modules/log"
	"forgejo.org/modules/setting"
	cluster_service "forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus"
)

type status string

const (
	// Pass healthy (acceptable aliases: "ok" to support Node's Terminus and "up" for Java's SpringBoot)
	// fail unhealthy (acceptable aliases: "error" to support Node's Terminus and "down" for Java's SpringBoot), and
	// warn healthy, with some concerns.
	//
	// ref https://datatracker.ietf.org/doc/html/draft-inadarei-api-health-check#section-3.1
	// status: (required) indicates whether the service status is acceptable
	// or not.  API publishers SHOULD use following values for the field:
	// The value of the status field is case-insensitive and is tightly
	// related with the HTTP Response code returned by the health endpoint.
	// For "pass" status, HTTP Response code in the 2xx-3xx range MUST be
	// used.  For "fail" status, HTTP Response code in the 4xx-5xx range
	// MUST be used.  In case of the "warn" status, endpoints MUST return
	// HTTP status in the 2xx-3xx range, and additional information SHOULD
	// be provided, utilizing optional fields of the Response.
	Pass status = "pass"
	Fail status = "fail"
	warn status = "warn"
)

func (s status) ToHTTPStatus() int {
	if s == Pass || s == warn {
		return http.StatusOK
	}
	return http.StatusFailedDependency
}

type checks map[string][]componentStatus

// Response is the data returned by the health endpoint, which will be marshaled to JSON format
type Response struct {
	Status      status `json:"status"`
	Description string `json:"description"`      // a human-friendly description of the service
	Checks      checks `json:"checks,omitempty"` // The Checks Object, should be omitted on installation route
}

// componentStatus presents one status of a single check object
// an object that provides detailed health statuses of additional downstream systems and endpoints
// which can affect the overall health of the main API.
type componentStatus struct {
	Status status `json:"status"`
	Time   string `json:"time"`             // the date-time, in ISO8601 format
	Output string `json:"output,omitempty"` // this field SHOULD be omitted for "pass" state.
}

// Check is the health check API handler
func Check(w http.ResponseWriter, r *http.Request) {
	rsp := Response{
		Status:      Pass,
		Description: setting.AppName,
		Checks:      make(checks),
	}

	statuses := make([]status, 0)
	if setting.InstallLock {
		statuses = append(statuses, checkDatabase(r.Context(), rsp.Checks))
		statuses = append(statuses, checkCache(rsp.Checks))
	}
	if r := cluster_service.Current(); r != nil {
		statuses = append(statuses, checkCluster(r, rsp.Checks))
	}
	for _, s := range statuses {
		if s != Pass {
			rsp.Status = Fail
			break
		}
	}

	data, _ := json.MarshalIndent(rsp, "", "  ")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(rsp.Status.ToHTTPStatus())
	_, _ = w.Write(data)
}

// database checks gitea database status
func checkDatabase(ctx context.Context, checks checks) status {
	st := componentStatus{}
	if err := db.GetEngine(ctx).Ping(); err != nil {
		st.Status = Fail
		st.Time = getCheckTime()
		log.Error("database ping failed with error: %v", err)
	} else {
		st.Status = Pass
		st.Time = getCheckTime()
	}

	if setting.Database.Type.IsSQLite3() && st.Status == Pass {
		if !setting.EnableSQLite3 {
			st.Status = Fail
			st.Time = getCheckTime()
			log.Error("SQLite3 health check failed with error: %v", "this Forgejo binary is built without SQLite3 enabled")
		} else {
			if _, err := os.Stat(setting.Database.Path); err != nil {
				st.Status = Fail
				st.Time = getCheckTime()
				log.Error("SQLite3 file exists check failed with error: %v", err)
			}
		}
	}

	checks["database:ping"] = []componentStatus{st}
	return st.Status
}

// cache checks gitea cache status
func checkCache(checks checks) status {
	st := componentStatus{}
	if err := cache.GetCache().Ping(); err != nil {
		st.Status = Fail
		st.Time = getCheckTime()
		log.Error("cache ping failed with error: %v", err)
	} else {
		st.Status = Pass
		st.Time = getCheckTime()
	}
	checks["cache:ping"] = []componentStatus{st}
	return st.Status
}

// checkCluster reports ready only when the replicator is ready: engine
// ready, table rebuilt, projection caught up once since the start (kitsunium
// ADR 0152 D19, R4-18). The failure tolerance, the rolling update's gate
// (R7-45), is known by the leader alone: the leader warns below one, a
// follower reports its own readiness and whether it knows a leader.
func checkCluster(r *cluster_service.Replicator, checks checks) status {
	st := componentStatus{Status: Pass, Time: getCheckTime()}
	if !r.Ready() {
		st.Status, st.Output = Fail, "the replicator is not ready"
	}
	checks["cluster:ready"] = []componentStatus{st}
	ft := componentStatus{Status: Pass, Time: getCheckTime()}
	if eng := r.Engine(); eng != nil {
		h := eng.Health()
		if h.Role == consensus.RoleLeader {
			ft.Output = fmt.Sprintf("role=leader failure_tolerance=%d", h.FailureTolerance)
			if h.FailureTolerance < 1 {
				ft.Status = warn
			}
		} else {
			ft.Output = fmt.Sprintf("role=%s leader_known=%t", h.Role, h.LeaderKnown)
			if !h.LeaderKnown {
				ft.Status = warn
			}
		}
	}
	checks["cluster:failure_tolerance"] = []componentStatus{ft}
	checkClusterSQL(r, checks)
	return st.Status
}

// checkClusterSQL warns while SQL is not durable across a failover of the
// database (the event queue is then not trimmed, and fills) and while
// events wait parked for their post-receive work.
func checkClusterSQL(r *cluster_service.Replicator, checks checks) {
	v := r.ConsumerHealth(context.Background())
	du := componentStatus{Status: Pass, Time: getCheckTime()}
	switch {
	case v.SQLDurabilityChecked.IsZero():
		du.Output = "not checked yet"
	case !v.SQLDurable:
		du.Status, du.Output = warn, "not durable across a database failover, the event queue is not trimmed: "+v.SQLDurability
	default:
		du.Output = v.SQLDurability
	}
	checks["cluster:sql_durability"] = []componentStatus{du}
	checks["cluster:parked_events"] = []componentStatus{parkedEvents(v)}
	bk := componentStatus{Status: Pass, Time: getCheckTime()}
	if v.BackupLag > 0 {
		bk.Status, bk.Output = warn, fmt.Sprintf("the PostgreSQL backup of the last checkpoint holds the trim: %d entries and %d bytes of the event queue behind it, pushes are refused at %d bytes; %s",
			v.BackupLag, v.BackupHeld, v.PushRefusedBytes, checkpointRemedy(v))
	}
	checks["cluster:backup_lag"] = []componentStatus{bk}
	ck := componentStatus{Status: Pass, Time: getCheckTime()}
	switch {
	case v.CheckpointsOff:
		ck.Status, ck.Output = warn, fmt.Sprintf("this leader takes no checkpoint ([cluster] CHECKPOINT_INTERVAL is 0 here) while the backup cursor of the last checkpoint stands at %d: the event queue is not trimmed past it; %s", v.Backup, takersNote(v))
	case v.CheckpointsHere:
		ck.Output = "taken by this node while it leads"
	default:
		ck.Output = "not taken by this node"
	}
	checks["cluster:checkpoints"] = []componentStatus{ck}
	q := componentStatus{Status: Pass, Time: getCheckTime(), Output: fmt.Sprintf("%d of %d bytes", v.EventBytes, v.EventQueueMax)}
	if v.EventQueueAlarm {
		q.Status, q.Output = warn, fmt.Sprintf("the event queue holds %d of %d bytes: pushes are refused past %d; %s", v.EventBytes, v.EventQueueMax, v.PushRefusedBytes, queueHolder(v))
	}
	checks["cluster:event_queue"] = []componentStatus{q}
}

// parkedEvents is the check of the parked events: a warning while some
// wait, which names, on the leader, one waiting for its repository's
// projection on it, that neither a retry nor reconcile applies (#249).
func parkedEvents(v cluster_service.View) componentStatus {
	pk := componentStatus{Status: Pass, Time: getCheckTime()}
	switch {
	case v.ParkedEvents < 0:
		pk.Output = "unknown"
	case v.ParkedEvents > 0 && v.ProjectionWait != nil:
		// No retry nor reconcile applies it while this node leads (#249).
		w := v.ProjectionWait
		pk.Status, pk.Output = warn, fmt.Sprintf("%d events wait parked for their post-receive work; the event queue is not trimmed past them; the event of %s at %d waits for its projection on this node, the leader, since %s, which `forgejo cluster reconcile` does not change: %s",
			v.ParkedEvents, w.Repo, w.Index, w.Since.UTC().Format(time.RFC3339), projectionRemedy(v))
	case v.ParkedEvents > 0:
		pk.Status, pk.Output = warn, fmt.Sprintf("%d events wait parked for their post-receive work; the event queue is not trimmed past them; see `forgejo cluster reconcile`", v.ParkedEvents)
	default:
		pk.Output = "none"
	}
	return pk
}

// checkpointRemedy is the way out of a trim held by the backup cursor; the
// status page chooses its text the same way (View.CheckpointsUntaken).
func checkpointRemedy(v cluster_service.View) string {
	if v.CheckpointsUntaken() {
		return "set [cluster] CHECKPOINT_INTERVAL on the leader, or transfer the leadership to a member whose CHECKPOINT_INTERVAL is set"
	}
	return "no checkpoint closed since it: see why in the leader's log (cluster checkpoint)"
}

// projectionRemedy is the way out of a parked event waiting for its
// repository's projection on the leader: the leadership of a member that
// projects it.
func projectionRemedy(v cluster_service.View) string {
	if v.ProjectionYield {
		return "the autopilot hands the leadership to a voter that says it projects it"
	}
	return "transfer the leadership to a member that projects it"
}

// takersNote says who may take the checkpoints, from what the leader last
// found: a voter that takes them may lead instead, unless the leader would
// not hand it the leadership (lag, contact, snapshot, a hand-over for the
// SQL), which it says; or none takes them.
func takersNote(v cluster_service.View) string {
	switch {
	case len(v.CheckpointTakers) > 0:
		s := fmt.Sprintf("the leadership may go to a voter that takes them (%s)", strings.Join(v.CheckpointTakers, ", "))
		if len(v.CheckpointTakersHeld) > 0 {
			s += "; not handed to it for now: " + strings.Join(v.CheckpointTakersHeld, "; ")
		}
		return s
	case v.CheckpointsAsked.IsZero():
		return checkpointRemedy(v)
	case len(v.CheckpointUnknown) > 0:
		return fmt.Sprintf("no voter that answered takes them, and %s did not say: set [cluster] CHECKPOINT_INTERVAL on a voter", strings.Join(v.CheckpointUnknown, ", "))
	default:
		return "no voter takes them ([cluster] CHECKPOINT_INTERVAL is 0 on every voter), and every write is refused once the queue is full: set CHECKPOINT_INTERVAL on a voter"
	}
}

// queueHolder names what holds the trim of a filling event queue, and the
// check that says more. SQL is said not durable only once checked, as
// cluster:sql_durability says it.
func queueHolder(v cluster_service.View) string {
	switch {
	case v.BackupLag > 0:
		return "the backup of the last checkpoint holds the trim (cluster:backup_lag)"
	case v.ParkedEvents > 0 && v.ProjectionWait != nil:
		return "a parked event waiting for its projection on this leader holds the trim (cluster:parked_events): " + projectionRemedy(v)
	case v.ParkedEvents > 0:
		return "parked events hold the trim (cluster:parked_events)"
	case !v.SQLDurable && !v.SQLDurabilityChecked.IsZero():
		return "SQL is not durable across a database failover, the trim waits for it (cluster:sql_durability)"
	case v.CheckpointsOff:
		return "the backup of the last checkpoint holds the trim and no checkpoint will move it (cluster:checkpoints)"
	case v.SQLDurabilityChecked.IsZero():
		return "the durability of SQL across a database failover is not checked yet, and the trim waits for it (cluster:sql_durability)"
	default:
		return "the SQL of the events is not derived yet: see the leader's consumer in its log"
	}
}

func getCheckTime() string {
	return time.Now().UTC().Format(time.RFC3339)
}
