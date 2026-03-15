package doris

import (
	"database/sql"
	"fmt"
)

// TxWithConn wraps sqlx.Tx along with the underlying *sql.Conn so we can ensure
// the connection is closed when Commit or Rollback is called.
type TxWithConn struct {
	Tx   *sql.Tx
	conn *sql.Conn
}

// Commit commits the transaction and closes the underlying connection.
func (t *TxWithConn) Commit() error {
	if t == nil || t.Tx == nil {
		return fmt.Errorf("nil tx")
	}
	err := t.Tx.Commit()
	cerr := t.conn.Close()
	if err != nil {
		if cerr != nil {
			return fmt.Errorf("commit error: %v; close error: %v", err, cerr)
		}
		return err
	}
	return cerr
}

// Rollback rollbacks the transaction and closes the underlying connection.
func (t *TxWithConn) Rollback() error {
	if t == nil || t.Tx == nil {
		return fmt.Errorf("nil tx")
	}
	err := t.Tx.Rollback()
	cerr := t.conn.Close()
	if err != nil {
		if cerr != nil {
			return fmt.Errorf("rollback error: %v; close error: %v", err, cerr)
		}
		return err
	}
	return cerr
}
