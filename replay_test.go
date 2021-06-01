package replay

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSharding(t *testing.T) {
	runs := []interface{}{1, 2, 3, 1.1, 2.2, time.Now(), time.Now().Unix(), time.Now().UnixNano()}
	for _, shards := range []int{1, 4, 16} {
		for _, run := range runs {
			var found bool
			for m := 0; m < shards; m++ {
				o := defaultOptions()
				WithHashedShard(m, shards)(&o)
				if o.shardFunc(fmt.Sprint(run)) {
					if found {
						require.Fail(t, "duplicate ofShard")
					}
					found = true
				}
			}
			if !found {
				require.Fail(t, "missing ofShard")
			}
		}
	}
}
