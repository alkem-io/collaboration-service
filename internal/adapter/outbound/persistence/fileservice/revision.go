package fileservice

import (
	"sync/atomic"

	"github.com/antst/go-yjs/backend/persistence"
)

// revisionCounter issues strictly increasing, process-local revisions.
//
// The contract requires revisions to increase per document; it does not require
// them to survive a restart, and file-service's own row Version is its
// concurrency token rather than ours — it is not returned by the content-write
// path, so it cannot serve as the revision even if we wanted it to.
type revisionCounter struct{ n atomic.Uint64 }

func (c *revisionCounter) next() persistence.Revision { return persistence.Revision(c.n.Add(1)) }
func (c *revisionCounter) current() persistence.Revision {
	return persistence.Revision(c.n.Load())
}
