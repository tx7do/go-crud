package cassandra

import (
	"github.com/gocql/gocql"
)

// Rows wraps a gocql iterator returned by Client.Query.
//
// The caller MUST call Close when finished iterating, otherwise the
// underlying connection is leaked. Close is safe to call multiple times.
type Rows struct {
	iter *gocql.Iter
}

// Columns returns the metadata of the columns in the current result set.
func (r *Rows) Columns() []gocql.ColumnInfo {
	if r == nil || r.iter == nil {
		return nil
	}
	return r.iter.Columns()
}

// Scan copies the next row into dest and returns true if a row was read.
// It returns false at end of results or on error.
func (r *Rows) Scan(dest ...any) bool {
	if r == nil || r.iter == nil {
		return false
	}
	return r.iter.Scan(dest...)
}

// PageState returns the paging state for resuming iteration on a subsequent
// query. Returns nil if paging is not in use or the result set is exhausted.
func (r *Rows) PageState() []byte {
	if r == nil || r.iter == nil {
		return nil
	}
	return r.iter.PageState()
}

// Close releases the underlying iterator. It must be called exactly once when
// the caller is done with the rows. Subsequent calls are no-ops.
func (r *Rows) Close() error {
	if r == nil || r.iter == nil {
		return nil
	}
	err := r.iter.Close()
	r.iter = nil
	return err
}
