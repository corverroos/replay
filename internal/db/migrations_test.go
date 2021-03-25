package db

import (
	"flag"
	"github.com/corverroos/truss"
	"testing"
)

var update = flag.Bool("update", false, "update schema file")

//go:generate go test -update -run=TestSchema

func TestSchema(t *testing.T) {
	truss.TestSchema(t, "schema.sql", *update, migrations...)
}
