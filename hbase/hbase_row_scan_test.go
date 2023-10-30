package hbase_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/hbase"
	"github.com/stretchr/testify/require"
)

// TODO: ADD PREPARE data

func TestHbaseScan(t *testing.T) {
	tbName := "hbtable"
	hbasePath := "localhost:2181"
	hbase.InitHbaseClient(hbasePath)
	scanner := hbase.TableScanRangeOpen(tbName, "8000000000000001", "8000000000000011")
	for {
		r := hbase.TableScanRangeNext(tbName, "", "", scanner)
		if nil == r {
			break
		}
		hbase.Cell2Map(r)
	}
	err := hbase.TableScanRangeClose(tbName, "", "", scanner)
	require.NoErrorf(t, err, "source %v", "should no error", errors.Trace(err))
}

func TestHbaseScan2(t *testing.T) {
	tbName := "hbtable"
	hbasePath := "localhost:2181"
	hbase.InitHbaseClient(hbasePath)
	scanner := hbase.TableScanRangeOpen(tbName, "1", "12345")
	for {
		r := hbase.TableScanRangeNext(tbName, "", "", scanner)
		if nil == r {
			break
		}
		hbase.Cell2Map(r)
	}
	err := hbase.TableScanRangeClose(tbName, "", "", scanner)
	require.NoErrorf(t, err, "source %v", "should no error", errors.Trace(err))
}
