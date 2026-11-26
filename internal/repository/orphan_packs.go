package repository

import (
	"context"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
)

// CleanupOrphanPacks removes pack files that exist in the backend but are not
// referenced by any index. Such orphaned packs are left behind when an upload
// succeeds but the index update fails, or when a previous backup was
// interrupted between uploading a pack and flushing the index.
//
// Every pack name is the SHA-256 of its contents, so an unreferenced pack can
// never become valid later on: any future upload of the same data produces the
// same name and a different pack would have a different name. It is therefore
// safe to delete unreferenced packs during the next backup.
//
// The repository must hold an exclusive lock and the index must have been
// loaded before calling this method.
func (r *Repository) CleanupOrphanPacks(ctx context.Context) error {
	indexedPacks := r.idx.Packs(nil)

	debug.Log("looking for orphaned packs (%d packs referenced in index)", len(indexedPacks))

	var orphans []restic.ID
	err := r.List(ctx, restic.PackFile, func(id restic.ID, _ int64) error {
		if !indexedPacks.Has(id) {
			orphans = append(orphans, id)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(orphans) == 0 {
		return nil
	}

	debug.Log("removing %d orphaned pack(s)", len(orphans))
	for _, id := range orphans {
		h := backend.Handle{Type: backend.PackFile, Name: id.String()}
		if err := r.be.Remove(ctx, h); err != nil && !r.be.IsNotExist(err) {
			// another process may have removed the pack in the meantime
			debug.Log("removing orphaned pack %v failed: %v", id, err)
			return err
		}
		debug.Log("removed orphaned pack %v", id)
	}

	return nil
}
