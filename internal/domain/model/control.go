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
	// read-only-state / room-user-change / session-end). Server-originated.
	WireControl WireMessageType = 3
	// WireDurabilityRequest carries a CLIENT→SERVER request to be told when the
	// document's current state is durable: a JSON body naming a caller-chosen
	// request id.
	//
	// It is a distinct wire type rather than an inbound WireControl because
	// WireControl is declared server→client and the inbound dispatch drops it by
	// design; widening that direction would make every control kind inbound-
	// reachable, which is a much larger contract change than this needs.
	//
	// ADDITIVE AND ROLLING-SAFE IN ONE DIRECTION ONLY: a client that never sends
	// type 4 is unaffected, and this service must therefore be deployed BEFORE any
	// caller that sends it. A caller that sends it to an older build reaches the
	// dispatch default and is silently ignored, which is why the caller must treat
	// a missing response as a failure and MUST NOT fall back to the room-wide
	// `saved` broadcast — that broadcast answers a different question and would
	// report another editor's save as this request's durability.
	WireDurabilityRequest WireMessageType = 4
	// WireHeartbeat is a client-originated liveness probe echoed to that same
	// connection. It carries no document state and is never broadcast or saved.
	WireHeartbeat WireMessageType = 5
)

// ControlKind names a server→client control event carried inside a WireControl
// message payload (contracts/ws-protocol.md type 3). The payload is a JSON
// ControlMessage so the set is extensible without a new wire type.
type ControlKind string

const (
	// ControlAdmission is the first frame of every accepted session. It makes the
	// immutable read/write capability explicit before either side starts Yjs sync.
	ControlAdmission ControlKind = "admission"
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
	// ControlUpdateRejected is emitted alongside the typed content-refused end for
	// one rolling-deployment window. Older browser bundles use it to stop editing;
	// new consumers use the session end. Remove after both consumers deploy.
	ControlUpdateRejected ControlKind = "update-rejected"
	// ControlSessionEnd tells a client its session is over, and why, in TYPED
	// fields it can branch on: Code (what happened), Scope (whether the whole
	// document ended or only this one member), and Disposition (what the client
	// should do about it). It is always followed by a WebSocket close whose status
	// is derived from the same Disposition. The control is queued first and the
	// close behind it on the same per-connection FIFO, so a client that reads up
	// to the close always has the reason for it.
	//
	// It replaces the former `room-closed`, which forced clients to string-match a
	// human-readable Error to decide whether to reconnect — and which carried
	// neither a reason nor an error at all on graceful shutdown, the one case
	// where reconnecting is exactly right.
	//
	// The literals matter: `client-web` keys off this kind and off every Code,
	// Scope, and Disposition value. Changing one is a cross-repo change.
	ControlSessionEnd ControlKind = "session-end"

	// ControlPersisted answers ONE durability request: the document state that
	// included the requester's preceding mutation has been accepted by BOTH
	// configured stores. It carries the RequestID it answers, so it cannot be
	// confused with the room-wide `saved` broadcast, which names no requester and
	// may be another editor's save entirely.
	//
	// It is NOT an acknowledgement of an individual update. One barrier may cover
	// any number of mutations, and a client may have only one outstanding.
	ControlPersisted ControlKind = "persisted"
	// ControlPersistFailed answers ONE durability request that cannot be
	// satisfied: the save failed, the room ended, the session had a frame dropped,
	// or the requester may not write. It carries the RequestID and a reason.
	//
	// Every request gets exactly one of persisted / persist-failed. Silence is
	// never an outcome the server intends, so a caller that sees silence has
	// grounds to treat the barrier as failed rather than guess.
	ControlPersistFailed ControlKind = "persist-failed"
)

// AdmissionMode is the immutable content capability declared before sync. It is
// deliberately not CollaboratorMode: admission is a transport contract about
// read/write, while viewer/collaborator is legacy product vocabulary retained
// only by ControlCollaboratorMode during the rolling compatibility window.
type AdmissionMode = string

const (
	// AdmissionRead grants sync without document mutation.
	AdmissionRead AdmissionMode = "read"
	// AdmissionWrite grants sync and document mutation.
	AdmissionWrite AdmissionMode = "write"
)

// SessionEndCode names WHAT ended a session. It is a closed set: every value is
// listed in sessionEndTable below, which is the single source of the scope and
// disposition that go with it, so two emitters cannot describe the same code
// differently.
type SessionEndCode = string

const (
	// CodeUpdateRateExceeded means this member exceeded its update rate budget. The
	// token bucket refills on its own, so reconnecting after a backoff genuinely
	// resolves it.
	CodeUpdateRateExceeded SessionEndCode = "update-rate-exceeded"
	// CodeDocumentSizeLimitExceeded means this member sent an update that would push
	// the document past MAX_DOC_BYTES. The update was refused BEFORE it was
	// applied, so the document never grew and reconnecting works — but the
	// offending local edit is still there, and replaying it trips this again
	// immediately. Hence DispositionManual, not Transient: the client must drop
	// that generation first, and a human must decide to come back.
	CodeDocumentSizeLimitExceeded SessionEndCode = "document-size-limit-exceeded"
	// CodeDocumentDeleted means the owner deleted the document. There is nothing
	// to come back to: a reconnect is refused by the existence gate, in every
	// deployment and regardless of authorization mode, so an automatic retry is
	// not merely pointless but a loop against a permanent refusal.
	CodeDocumentDeleted SessionEndCode = "document-deleted"
	// CodeEditsNotSaved means the in-memory document was abandoned WITHOUT being
	// persisted, so edits made since the last successful flush are gone. All three
	// no-flush teardowns report it (FR-011a groups them for exactly this reason):
	// durability escalation, registry generation invalidation, and teardown after
	// a panic on the document's own processing path. The client must surface data
	// loss rather than silently reconnecting into an older document.
	CodeEditsNotSaved SessionEndCode = "edits-not-saved"
	// CodeServerShutdown means the service is going away (graceful shutdown, or an
	// idle room being released). The document is intact and was flushed; coming
	// back with a backoff is the correct behaviour.
	CodeServerShutdown SessionEndCode = "server-shutdown"

	// CodeUpdateNotAccepted means an inbound update was refused by BACKPRESSURE:
	// the room was ALIVE and its command buffer stayed full past the deadline. The
	// update was NOT applied, NOT broadcast and NOT saved.
	//
	// BACKPRESSURE ONLY, and the narrowness is load-bearing. An enqueue is also
	// refused when the room has left Active, and that case is deliberately SILENT
	// here: the room is tearing down and will send its own AUTHORITATIVE
	// document-scoped end (document-deleted, edits-not-saved, server-shutdown). A
	// member-scoped transient end from the refusal path would reach the
	// connection's terminal boundary first and make the real one be refused — so a
	// deletion or a data-loss escalation would reach the user as "try again later".
	// Teardown owns its own ending; this code speaks only for a live room that
	// could not keep up.
	//
	// MEMBER scope: this is one connection's frame, and the room and every other
	// member are unaffected. TRANSIENT disposition: a momentary server-side
	// backlog, so the client should reconnect with backoff rather than treat it as
	// final.
	//
	// It exists because the alternative was silence. The refusal used to be
	// discarded, so the client kept editing a generation the server never received
	// — a divergence nothing would report, and one that would let a durability
	// barrier answer "your work is safe" about a mutation that never arrived.
	// Naming it is what makes the loss visible at the moment it happens.
	//
	// DEPLOY ORDER MATTERS: a client that does not know this code classifies it as
	// unknown and fails CLOSED (terminal, no reconnect), which is the opposite of
	// the intent. Clients must understand it BEFORE this service emits it.
	CodeUpdateNotAccepted SessionEndCode = "update-not-accepted"
	// CodeContentRefused means a client update failed the content contract before
	// application. Replaying the same local state would be refused again, so the
	// client retains it for export and reloads only by explicit user action.
	CodeContentRefused SessionEndCode = "content-refused"
	// CodeForbidden means a read-admitted connection attempted to mutate content.
	// Honest clients cannot produce it; a buggy or malicious viewer is terminated.
	CodeForbidden SessionEndCode = "forbidden"
)

// SessionEndScope says WHO the end applies to, so a client can tell "the
// document is over for everyone" from "you personally were dropped, and the
// room is still serving other people".
type SessionEndScope = string

const (
	// ScopeMember means only this connection ended. The room stays alive and other
	// members are untouched.
	ScopeMember SessionEndScope = "member"
	// ScopeDocument means the room itself ended; every member gets the same code.
	ScopeDocument SessionEndScope = "document"
)

// SessionEndDisposition says what the CLIENT should do, so that decision is not
// re-derived from the code by every consumer. Three values, because there are
// exactly three distinct client behaviours.
type SessionEndDisposition = string

const (
	// DispositionTransient means reconnect on a backoff. The condition clears by
	// itself (a restarted pod, a refilled rate bucket).
	DispositionTransient SessionEndDisposition = "transient"
	// DispositionManual means do NOT reconnect automatically. Reconnecting would
	// succeed and then immediately fail the same way; the client must discard the
	// offending local generation and let a human choose to return.
	DispositionManual SessionEndDisposition = "manual"
	// DispositionTerminal means do not reconnect at all. Either there is nothing to
	// connect to, or the caller is not allowed to.
	DispositionTerminal SessionEndDisposition = "terminal"
)

// SessionEnd is the domain-side close intent: the reason a session ended,
// carried from the room to the transport adapter. It holds no transport type —
// the WebSocket status is derived from Disposition by the adapter — so the
// domain never imports a websocket package (constitution §I).
type SessionEnd struct {
	Code        SessionEndCode
	Scope       SessionEndScope
	Disposition SessionEndDisposition
}

// sessionEndTable is the ONE place a code's scope and disposition are decided.
// Emitters name a code and get the rest, so the wire contract cannot drift
// between the four teardown paths and the two per-member limits.
var sessionEndTable = map[SessionEndCode]SessionEnd{
	CodeUpdateRateExceeded:        {CodeUpdateRateExceeded, ScopeMember, DispositionTransient},
	CodeDocumentSizeLimitExceeded: {CodeDocumentSizeLimitExceeded, ScopeMember, DispositionManual},
	CodeDocumentDeleted:           {CodeDocumentDeleted, ScopeDocument, DispositionTerminal},
	CodeEditsNotSaved:             {CodeEditsNotSaved, ScopeDocument, DispositionTerminal},
	CodeServerShutdown:            {CodeServerShutdown, ScopeDocument, DispositionTransient},
	CodeUpdateNotAccepted:         {CodeUpdateNotAccepted, ScopeMember, DispositionTransient},
	CodeContentRefused:            {CodeContentRefused, ScopeMember, DispositionManual},
	CodeForbidden:                 {CodeForbidden, ScopeMember, DispositionTerminal},
}

// NewSessionEnd resolves a code to its full session-end intent. An unknown code
// panics rather than returning a zero value: the set is closed and known at
// compile time, so an unlisted code is a programming error, and a zero-valued
// SessionEnd would otherwise reach a client as an empty disposition it cannot
// act on.
func NewSessionEnd(code SessionEndCode) SessionEnd {
	end, ok := sessionEndTable[code]
	if !ok {
		panic("model: unknown session-end code " + code)
	}
	return end
}

// SessionEndCodes lists every code, so a test can assert the set is exhaustively
// handled instead of trusting that a new one was remembered everywhere.
func SessionEndCodes() []SessionEndCode {
	codes := make([]SessionEndCode, 0, len(sessionEndTable))
	for code := range sessionEndTable {
		codes = append(codes, code)
	}
	return codes
}

// Control returns the control message that announces this session end.
func (e SessionEnd) Control() ControlMessage {
	return ControlMessage{
		Kind:        ControlSessionEnd,
		Code:        e.Code,
		Scope:       e.Scope,
		Disposition: e.Disposition,
	}
}

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
	// ReasonNoUpdateAccess marks a read-only client whose authZ granted read but
	// denied update-content, so the actor is a viewer (legacy "no-update-access").
	ReasonNoUpdateAccess ReadOnlyReason = "no-update-access"
	// ReasonRoomCapacityReached marks a read-only/refused client because the room
	// is at its connection cap (ROOM_CAPACITY_REACHED).
	ReasonRoomCapacityReached ReadOnlyReason = "room-capacity-reached"
	// ReasonMultiUserNotAllowed marks the second writer on a document whose
	// server-owned license permits only one live editor.
	ReasonMultiUserNotAllowed ReadOnlyReason = "multi-user-not-allowed"
)

// CollaboratorModeReason is the code (OPEN-1) explaining a collaborator-mode
// downgrade, carried in ControlMessage.Reason on a ControlCollaboratorMode. It
// is the subset of reasons that drive today's collaborator-mode UX:
// capacity/multi-user contention and inactivity.
type CollaboratorModeReason = string

const (
	// ReasonInactivity marks an idle collaborator downgraded to viewer after an
	// explicitly configured inactivity window (FR-014).
	ReasonInactivity CollaboratorModeReason = "inactivity"
	// ReasonRoomCapacityReached is shared with the
	// read-only reasons above; they also surface as collaborator-mode reasons.
)

// ControlMessage is the JSON body of a WireControl message. Only the fields
// relevant to Kind are populated; the rest are omitted.
type ControlMessage struct {
	// Kind selects the control event.
	Kind ControlKind `json:"kind"`
	// RequestID correlates ControlPersisted / ControlPersistFailed with the
	// durability request that asked. Empty on every other kind — in particular on
	// ControlSaved, which is a room-wide broadcast and answers nobody.
	RequestID string `json:"requestId,omitempty"`
	// Version is the persisted snapshot version for ControlSaved.
	Version int `json:"version,omitempty"`
	// Error is a human-readable reason on any control that refuses or reports a
	// failure. NEVER secrets, and never a stable identifier: clients branch on
	// Kind (and on Code for a session end), never on this prose, so it may be
	// reworded freely.
	//
	// Carried today by ControlSaveError, ControlPersistFailed, and the temporary
	// ControlUpdateRejected compatibility frame. Listed
	// because the set has already drifted twice; the RULE is what governs, so a
	// new refusal kind should carry it without needing this comment changed.
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
	// Mode is kind-specific: ControlAdmission carries read/write, while the
	// temporary ControlCollaboratorMode compatibility frame carries the legacy
	// viewer/collaborator vocabulary. Consumers always branch on Kind first.
	Mode string `json:"mode,omitempty"`
	// Users is the current participant count for ControlRoomUserChange.
	Users int `json:"users,omitempty"`
	// Code is the SessionEndCode on a ControlSessionEnd: what ended the session.
	Code SessionEndCode `json:"code,omitempty"`
	// Scope is the SessionEndScope on a ControlSessionEnd: whether the document
	// ended for everyone or only this member was dropped.
	Scope SessionEndScope `json:"scope,omitempty"`
	// Disposition is the SessionEndDisposition on a ControlSessionEnd: what the
	// client should do — reconnect on a backoff, wait for a human, or stop.
	Disposition SessionEndDisposition `json:"disposition,omitempty"`
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
