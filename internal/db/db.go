package db

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/corverroos/replay"
	"github.com/corverroos/truss"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

func ConnectForTesting(t *testing.T) *sql.DB {
	CleanCache(t)
	return truss.ConnectForTesting(t, migrations...)
}

type NoType struct {
}

func (n NoType) ReflexType() int {
	return 0
}

const ErrDupEntry = 1062

// MaybeWrapErrDuplicate returns ErrDuplicate and true if the provided error
// is a mysql ER_DUP_ENTRY error that conflicts with the specified
// unique index or primary key. Otherwise it returns the provided error and false.
func MaybeWrapErrDuplicate(err error, key string) (error, bool) {
	if err == nil {
		return nil, false
	}

	me := new(mysql.MySQLError)
	if !errors.As(err, &me) {
		return err, false
	}

	if me.Number != ErrDupEntry {
		return err, false
	}

	if !strings.Contains(me.Message, fmt.Sprintf("key '%s'", key)) {
		return err, false
	}

	return errors.Wrap(replay.ErrDuplicate, "duplicate for key", j.KS("key", key)), true
}
