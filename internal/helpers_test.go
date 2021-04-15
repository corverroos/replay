package internal

import (
	"fmt"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestKeyParse(t *testing.T) {
	tests := []struct {
		Key string
	}{
		{
			Key: "ns/w/r",
		}, {
			Key: "ns_1/w d/r123",
		}, {
			Key: "n/w/r?a=a&n=0",
		}, {
			Key: "n/w/r?n=99",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			k, err := DecodeKey(test.Key)
			jtest.RequireNil(t, err)
			require.Equal(t, test.Key, k.Encode())
		})
	}
}
