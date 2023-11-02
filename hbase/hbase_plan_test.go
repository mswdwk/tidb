package hbase_test

import (
	"testing"

	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// reference TestCrossValidationSelectivity

/*
	hbase table hb1 (dataSource s1):   id >  1 and id < 100
	tikv  table tb1 (dataSource s2):   id >=100
	hyper view   v1 :
		create view v1 as if  id >=120
			then
				select * from hb1
			else
				select * from tb1
			end

	INPUT SQL:
		select * from v1 where id > 50 and id < 200

	dataSource caculate:
		selections [ "id > 50" , "id < 200" ]
		              |               |
					  V               V
				   [s1,s2]         [s1,s2]
						 \        /
						   \    /
				              V
	               tablerefs:[s1,s2]
	PLAN:
		number: 2
		plan 1 (datasource s1): select * from hb1 where id > 50 and id < 200
		plan 2 (datasource s2): select * from tb1 where id > 50 and id < 200
*/

func TestSelection(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("create table t (a int, b int, c int, primary key (a, b) clustered)")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into t values (1,2,3), (1,4,5)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 and b > 0 and b < 1000 and c > 1000").Check(testkit.Rows(
		"TableReader 0.00 root  data:Selection",
		"└─Selection 0.00 cop[tikv]  gt(test.t.c, 1000)",
		"  └─TableRangeScan 2.00 cop[tikv] table:t range:(1 0,1 1000), keep order:false"))
}
