// Package redis will implement the multi-pod ClusterBroadcaster (port.ClusterBroadcaster)
// using Redis pub-sub: document updates published on doc:{id} and ephemeral/
// awareness messages on awareness:{id}, so clients connected to any pod
// converge transparently (R4). Implementation lands with task T004 of
// specs/003-unify-collab-yjs/tasks/collaboration-service.md.
package redis
