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
	// a granular code (OPEN-1) so the client can preserve its read-only UX
	// granularity (e.g. the memo footer readOnlyCode): a ReadOnlyReason for an
	// authZ-driven downgrade, or a CollaboratorModeReason (e.g. "inactivity") when
	// the read-only state is the result of a collaborator-mode downgrade.
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
	// ControlUpdateRejected tells one client its update was refused and NOT applied.
	// It is sent only to the sender: no other member saw the update, and telling
	// them about it would leak one client's failed edit to the room.
	//
	// The server does not close the connection: a rejected update is a refused
	// write, not grounds for disconnection.
	//
	// But the SENDER cannot simply carry on. The server is missing that client's
	// struct at clock k, so its next incremental struct at k+1 arrives with a gap in
	// front of it and stays pending rather than materializing. The sender must
	// discard that local generation and resync — recreate the editor — before it can
	// write again. That recovery is the client's, and this message is what tells it
	// to start.
	//
	// CONSUMED by `client-web`: 8d69ef4ff handles this kind by discarding the editor
	// generation and reloading server state, 5c6f4600f corrects its teardown. So the
	// contract is closed on both sides — the server refuses and says so, the client
	// resets rather than keeping an edit the document does not contain.
	//
	// The literal matters: the client keys off this exact kind string. Renaming it is
	// a cross-repo change, not a local one.
	ControlUpdateRejected ControlKind = "update-rejected"
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
	// ReasonNotYetDurable marks a save-error whose cause is transient: the
	// document is still being served and the flush is being retried, so the
	// client's recent edits exist but are not yet safe. It is deliberately
	// distinct from a terminal failure so a client can say "saving..." rather
	// than "save failed" (FR-027).
	ReasonNotYetDurable ReadOnlyReason = "not-yet-durable"
	// ReasonEditsNotSaved marks a room closed because repeated persist failures
	// crossed the escalation threshold and the unsaved edits were DISCARDED. It
	// must be distinguishable from an ordinary disconnect: the user lost work,
	// and a generic close reason would hide that (FR-028, SC-016).
	ReasonEditsNotSaved ReadOnlyReason = "edits-not-saved"
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
	// ReadOnly is the viewer/collaborator state for ControlReadOnlyState. It is a
	// pointer so the wire distinguishes three states: absent (nil — this frame
	// does not carry a read-only state, e.g. a saved/room-user-change frame),
	// explicit true (a downgrade to read-only), and explicit FALSE (an upgrade
	// REGAINING edit access). The false case MUST survive on the wire: with a plain
	// `bool,omitempty` a regain frame marshals to `{"kind":"read-only-state"}` —
	// the key omitted, shape-identical to a frame that never set it — so a JS/TS
	// client (no Go zero-value) never sees readOnly:false and stays locked
	// read-only after an access regrant until a full reconnect (the upgrade is
	// broken while the downgrade survives). A *bool keeps the false explicit while
	// still omitting the key from every non-read-only-state frame. Set it with
	// ReadOnlyState(true|false); read it with (ControlMessage).IsReadOnly().
	ReadOnly *bool `json:"readOnly,omitempty"`
	// Reason is the granular code (OPEN-1) explaining a downgrade. On a
	// ControlCollaboratorMode it is a CollaboratorModeReason; on a
	// ControlReadOnlyState it is usually a ReadOnlyReason, but an inactivity (or
	// other collaborator-mode) downgrade also rides on read-only-state carrying a
	// CollaboratorModeReason such as "inactivity". Both reason vocabularies are
	// string aliases, so the wire field is one string. It is additive and
	// backward-compatible: clients that ignore it still work. Omitted when empty.
	Reason string `json:"reason,omitempty"`
	// Mode is the resulting collaborator mode for ControlCollaboratorMode
	// (viewer/collaborator). Omitted when empty.
	Mode CollaboratorMode `json:"mode,omitempty"`
	// Users is the current participant count for ControlRoomUserChange.
	Users int `json:"users,omitempty"`
}

// ReadOnlyState returns a *bool for ControlMessage.ReadOnly so a read-only-state
// frame carries the value EXPLICITLY on the wire — including the regain case
// (false), which must not be dropped (see the ReadOnly field doc). Use it for
// every read-only-state frame so an upgrade (false) and a downgrade (true) are
// equally unambiguous to a non-Go client.
func ReadOnlyState(readOnly bool) *bool {
	return &readOnly
}

// IsReadOnly reports whether this control message carries an explicit read-only
// = true state. A nil ReadOnly (no read-only state on the frame) or an explicit
// false (a regain/upgrade) both report false. Callers that must distinguish "no
// state" from "explicitly editable" should inspect ReadOnly directly.
func (m ControlMessage) IsReadOnly() bool {
	return m.ReadOnly != nil && *m.ReadOnly
}
