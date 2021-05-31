package internal

import (
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"
)

//go:generate go test -update

func TestParse1(t *testing.T) {
	namespace, err := Parse("testdata/replay.go")
	jtest.RequireNil(t, err)

	jtest.RequireNil(t, Validate(namespace))

	goldie.New(t, goldie.WithNameSuffix(".json")).AssertJson(t, "namespace", namespace)

	b, err := Render(namespace, "testdata/replay_gen.go", true)
	jtest.RequireNil(t, err)

	goldie.New(t, goldie.WithNameSuffix(".go")).Assert(t, "replay_gen", b)
}

func TestParse2(t *testing.T) {
	namespace, err := Parse("testdata/expose/replay.go")
	jtest.RequireNil(t, err)

	jtest.RequireNil(t, Validate(namespace))

	goldie.New(t, goldie.WithFixtureDir("testdata/expose"), goldie.WithNameSuffix(".json")).AssertJson(t, "namespace", namespace)

	b, err := Render(namespace, "testdata/expose/replay_gen.go", true)
	jtest.RequireNil(t, err)

	goldie.New(t, goldie.WithFixtureDir("testdata/expose"), goldie.WithNameSuffix(".go")).Assert(t, "replay_gen", b)
}

func TestCamelSnake(t *testing.T) {
	tests := []struct {
		Snake, Pascal, Camel string
	}{
		{
			Snake:  "milk_man",
			Camel:  "milkMan",
			Pascal: "MilkMan",
		}, {
			Snake:  "test_v3",
			Camel:  "testV3",
			Pascal: "TestV3",
		},
	}

	for _, test := range tests {
		t.Run(test.Snake, func(t *testing.T) {
			require.Equal(t, test.Pascal, toPascal(test.Snake))
			require.Equal(t, test.Pascal, toPascal(test.Camel))
			require.Equal(t, test.Snake, toSnake(test.Pascal))
			require.Equal(t, test.Snake, toSnake(test.Camel))
			require.Equal(t, test.Camel, toCamel(test.Snake))
			require.Equal(t, test.Camel, toCamel(test.Pascal))
		})
	}
}
