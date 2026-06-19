package model

// WireMessageType is the leading type byte of a framed WebSocket message
// (contracts/ws-protocol.md "Messages"). Types 0 (sync) and 1 (awareness) are
// owned by the y-protocols layer; types 2 (ephemeral) and 3 (control) are this
// service's custom channels and are framed with the same
// [type as VarUint][payload] envelope the protocol package uses.
type WireMessageType uint8

const (
	// WireSync carries y-protocols sync sub-messages (SyncStep1 / SyncStep2 /
	// Update). Persisted via the debounced snapshot.
	WireSync WireMessageType = 0
	// WireAwareness carries y-protocols awareness updates (cursors, online,
	// idle, mode). TTL'd, never persisted (FR-008).
	WireAwareness WireMessageType = 1
	// WireEphemeral carries the custom whiteboard ephemeral events
	// (MOUSE_LOCATION, IDLE_STATUS, EMOJI_REACTION, COUNTDOWN_TIMER,
	// USER_VISIBLE_SCENE_BOUNDS). Volatile, lossy, fanned out, never persisted
	// (FR-008).
	WireEphemeral WireMessageType = 2
	// WireControl carries server→client control messages (saved / save-error /
	// read-only-state / room-user-change / room-closed). Server-originated.
	WireControl WireMessageType = 3
)

// ControlKind names a server→client control event carried inside a WireControl
// message payload (contracts/ws-protocol.md type 3). The payload is a JSON
// ControlMessage so the set is extensible without a new wire type.
type ControlKind string

const (
	// ControlSaved signals a debounced snapshot was persisted (R7). Carries the
	// persisted Version.
	ControlSaved ControlKind = "saved"
	// ControlSaveError signals a snapshot persist attempt failed; the room
	// keeps serving from memory and retries on the next debounce tick (R7).
	ControlSaveError ControlKind = "save-error"
	// ControlReadOnlyState tells a client whether it may mutate the document
	// (viewer vs collaborator). Wired fully with AuthZ in T014; emitted here so
	// the wire shape is exercised. When ReadOnly is true the Reason field carries
	// a ReadOnlyReason code (OPEN-1) so the client can preserve its read-only UX
	// granularity (e.g. the memo footer readOnlyCode).
	ControlReadOnlyState ControlKind = "read-only-state"
	// ControlCollaboratorMode tells a client its collaborator mode changed with a
	// reason (OPEN-1): a capacity/multi-user/inactivity downgrade carries a
	// CollaboratorModeReason so the client mirrors today's collaborator-mode UX.
	// Additive to ControlReadOnlyState — clients that only read read-only-state
	// keep working.
	ControlCollaboratorMode ControlKind = "collaborator-mode"
	// ControlRoomUserChange notifies clients that the room's participant set
	// changed (join/leave); carries the current participant count.
	ControlRoomUserChange ControlKind = "room-user-change"
	// ControlRoomClosed tells clients the room is being torn down (idle release
	// or owner delete); the client should stop sending and may reconnect.
	ControlRoomClosed ControlKind = "room-closed"
)

// ReadOnlyReason is the granular code (OPEN-1) explaining why a client is
// read-only, carried in ControlMessage.Reason on a ControlReadOnlyState whose
// ReadOnly is true. It maps 1:1 to today's memo-footer readOnlyCode so the
// WS-D client preserves its existing read-only UX. An empty Reason on a
// read-only frame is still valid (a client ignoring it sees plain read-only).
type ReadOnlyReason = string

const (
	// ReasonNotAuthenticated marks a read-only client that is not authenticated,
	// so it may view but not mutate (legacy readOnlyCode "not-authenticated").
	ReasonNotAuthenticated ReadOnlyReason = "not-authenticated"
	// ReasonNoUpdateAccess marks a read-only client whose authZ granted read but
	// denied update-content, so the actor is a viewer (legacy "no-update-access").
	ReasonNoUpdateAccess ReadOnlyReason = "no-update-access"
	// ReasonRoomCapacityReached marks a read-only/refused client because the room
	// is at its connection cap (ROOM_CAPACITY_REACHED).
	ReasonRoomCapacityReached ReadOnlyReason = "room-capacity-reached"
	// ReasonMultiUserNotAllowed marks a read-only/refused client because the
	// document is single-user (multiUserNotAllowed) and already occupied.
	ReasonMultiUserNotAllowed ReadOnlyReason = "multi-user-not-allowed"
)

// CollaboratorModeReason is the code (OPEN-1) explaining a collaborator-mode
// downgrade, carried in ControlMessage.Reason on a ControlCollaboratorMode. It
// is the subset of reasons that drive today's collaborator-mode UX:
// capacity/multi-user contention and inactivity.
type CollaboratorModeReason = string

const (
	// ReasonInactivity marks an idle collaborator downgraded to viewer after the
	// inactivity window (FR-014, whiteboard collaborator_inactivity parity).
	ReasonInactivity CollaboratorModeReason = "inactivity"
	// ReasonRoomCapacityReached and ReasonMultiUserNotAllowed are shared with the
	// read-only reasons above; they also surface as collaborator-mode reasons.
)

// ControlMessage is the JSON body of a WireControl message. Only the fields
// relevant to Kind are populated; the rest are omitted.
type ControlMessage struct {
	// Kind selects the control event.
	Kind ControlKind `json:"kind"`
	// Version is the persisted snapshot version for ControlSaved.
	Version int `json:"version,omitempty"`
	// Error is a human-readable reason for ControlSaveError (never secrets).
	Error string `json:"error,omitempty"`
	// ReadOnly is the viewer/collaborator state for ControlReadOnlyState.
	ReadOnly bool `json:"readOnly,omitempty"`
	// Reason is the granular code (OPEN-1) explaining a ControlReadOnlyState
	// (a ReadOnlyReason) or a ControlCollaboratorMode (a CollaboratorModeReason).
	// It is additive and backward-compatible: clients that ignore it still work.
	// Omitted when empty.
	Reason string `json:"reason,omitempty"`
	// Mode is the resulting collaborator mode for ControlCollaboratorMode
	// (viewer/collaborator). Omitted when empty.
	Mode CollaboratorMode `json:"mode,omitempty"`
	// Users is the current participant count for ControlRoomUserChange.
	Users int `json:"users,omitempty"`
}
