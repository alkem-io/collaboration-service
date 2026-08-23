package service

import "time"

// NopMetrics is the no-op Metrics sink used when no Metrics is wired (NewManager
// defaults to it). Every method is an empty no-op: a metrics-less deployment must
// absorb every observability hook without effect or panic. These bodies carry no
// logic and cannot fail, so they are kept in their own file and excluded from the
// coverage gate (constitution §XII: do not test code that cannot fail).
type NopMetrics struct{}

// RoomOpened does nothing.
func (NopMetrics) RoomOpened() {}

// RoomClosed does nothing.
func (NopMetrics) RoomClosed(string) {}

// ConnOpened does nothing.
func (NopMetrics) ConnOpened() {}

// ConnClosed does nothing.
func (NopMetrics) ConnClosed() {}

// SnapshotSaved does nothing.
func (NopMetrics) SnapshotSaved() {}

// SnapshotFailed does nothing.
func (NopMetrics) SnapshotFailed() {}

// FanoutPublished does nothing.
func (NopMetrics) FanoutPublished(time.Duration) {}

// FanoutFailed does nothing.
func (NopMetrics) FanoutFailed() {}

// ContributingActors does nothing.
func (NopMetrics) ContributingActors(int) {}

// DocumentUndurable does nothing.
func (NopMetrics) DocumentUndurable(string, int, time.Duration) {}

// DocumentDurabilityRestored does nothing.
func (NopMetrics) DocumentDurabilityRestored(string) {}

// DocumentEscalated does nothing.
func (NopMetrics) DocumentEscalated(string, time.Duration) {}
