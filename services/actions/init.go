// Copyright 2022 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package actions

import (
	"forgejo.org/modules/graceful"
	"forgejo.org/modules/log"
	"forgejo.org/modules/queue"
	"forgejo.org/modules/setting"
	notify_service "forgejo.org/services/notify"
)

func Init() {
	if !setting.Actions.Enabled {
		return
	}

	jobEmitterQueue = queue.CreateUniqueQueue(graceful.GetManager().ShutdownContext(), "actions_ready_job", jobEmitterQueueHandler)
	if jobEmitterQueue == nil {
		log.Fatal("Unable to create actions_ready_job queue")
	}
	go graceful.GetManager().RunWithCancel(jobEmitterQueue)

	// The events a node of a cluster deferred until a node serves their
	// repository fresh (deferred.go).
	go graceful.GetManager().RunWithShutdownContext(runDeferredReplayer)

	notify_service.RegisterNotifier(NewNotifier())
}
