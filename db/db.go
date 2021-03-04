package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/corverroos/truss"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

var (
	ErrNotFound  = errors.New("database entry not found", j.C("ERR_a4d9e7b9eb6b8b0c"))
	ErrDuplicate = errors.New("duplicate entry", j.C("ERR_96713b1c52c5d59f"))
)

func Connect(uri string) (*sql.DB, error) {
	dbc, err := truss.Connect(uri)
	if err != nil {
		return nil, err
	}

	err = truss.Migrate(context.Background(), dbc, migrations)
	if err != nil {
		return nil, err
	}

	return dbc, nil
}

func ConnectForTesting(t *testing.T) *sql.DB {
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
		return err, false
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

	return errors.Wrap(ErrDuplicate, "duplicate for key", j.KS("key", key)), true
}
