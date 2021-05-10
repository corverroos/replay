package db

import (
	"flag"
	"testing"

	"github.com/corverroos/truss"
)

var update = flag.Bool("update", false, "update schema file")

//go:generate go test -update -run=TestSchema

func TestSchema(t *testing.T) {
	t.Skip("TODO(): Fix test.")
	truss.TestSchema(t, "schema.sql", *update, migrations...)
}
