package hbase_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/hbase"
)

func TestEncode(t *testing.T) {
	var i int64 = 1
	var b []byte
	b = hbase.EncodeInt(b, i)
	fmt.Printf("%d = %x\n", i, b)
	i = -1
	var b2 []byte
	b2 = hbase.EncodeInt(b2, i)
	fmt.Printf("%d = %x\n", i, b2)

	var i2 uint64 = 0x8000000000000001
	b2 = b[:0]
	b2 = hbase.EncodeUint(b2, i2)

	fmt.Printf("%d = %x\n", i2, b2)
}
