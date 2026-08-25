# Adapter inventory — zero translation shims (T063, SC-008a)

**The claim under test.** Each adopted contract has exactly ONE implementation
per backend, and each reaches its infrastructure directly rather than through a
pre-existing adapter of ours. A shim is what §VIII forbids: a layer whose only
job is to make our old interface look like the core's, which leaves both alive
and makes the pair the real contract.

Verified by inspection of `internal/adapter/outbound/`, on 2026-08-19.

## Adopted core contracts

| Contract | Backend | Implementation | Reaches infrastructure directly? |
|---|---|---|---|
| `persistence.DeletingCheckpointStore` | file-service | `persistence/fileservice` | yes — `net/http` to `/internal/file` |
| `persistence.DeletingCheckpointStore` | in-process | `persistence/inprocess` | yes — a map |
| `hub.Hub` | Redis | `hub/redis` | yes — `go-redis` pub/sub |
| `hub.Hub` | single-pod | *(none — the core's `hub.NewInProcess()` is used unmodified)* | n/a |
| `memory.Registry` | in-process | *(none — the core's `memory.NewRegistry()` is used unmodified)* | n/a |

Two contracts have **no implementation of ours at all**. That is the strongest
form of the property: where the core ships something that fits, we use it rather
than reimplement it (§X/§XI), and the conformance suites are still run against
those shipped types so a future core bump cannot change them under us silently.

## This service's own ports (not core contracts)

| Port | Backends | Note |
|---|---|---|
| `port.MetadataStore` | `metadatastore/{inmemory,rabbitmq,postgres}` | the Alkemio document index; the core has no contract for it |
| `port.Auth` | `auth/{header,oidc,open}` | handshake authN |
| `port.AuthZ` | `auth/{authzeval,open}` | per-document authZ |
| `port.Contributor` | rabbitmq (the metadata store also satisfies it) | contribution events |

## The one adapter that imports another adapter

`persistence/metapointer` imports `persistence/fileservice`.

**It is not a shim, and the distinction is worth stating precisely.** A shim
would translate between two implementations of the SAME concern. This translates
between two DIFFERENT concerns that must meet somewhere: file-service assigns
file ids on create and accepts no caller-supplied id, so the
`DocumentID → file id` mapping has to live somewhere, and the Alkemio index
already carries it as `ContentPointer`. `metapointer` is the one place that fact
is read and written.

It imports `fileservice` only for that package's `PointerResolver` interface and
`ErrNoPointer` sentinel — the contract it satisfies — not to call any store
method. Removing it would not remove a layer; it would require inventing a second
home for the same mapping, which is the duplication §VIII actually cares about.

## Removed, not wrapped

Both superseded ports were deleted outright, with every adapter behind them:

- `port.BlobStore` and `blobstore/{inline,fileservice,s3,local}` → replaced by
  `persistence.CheckpointStore`. Two backends now, not four.
- `port.ClusterBroadcaster` and `fanout/{inmemory,redis}` → replaced by
  `hub.Hub`. The in-memory one has no successor because the core ships it.

Neither survives behind an adapter. `git log --diff-filter=D` shows the deletions.
