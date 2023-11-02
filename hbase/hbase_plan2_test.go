package hbase_test

import (
	"testing"

	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// reference TestCrossValidationSelectivity
// consider multi column primary key
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

func TestSelection2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists hb1")
	tk.MustExec("drop table if exists tb1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("create table hb1 (id int primary key, name varchar(64))")
	tk.MustExec("create table tb1 (id int primary key, name varchar(64))")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into tb1 values (10,'name 10'),(30,'name 30'),(60,'name 60'),(90,'name 90')")
	tk.MustExec("insert into hb1 values (100,'name 100'), (150,'name 150'),(200,'name 200'),(300,'name 300')")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table hb1")
	tk.MustExec("analyze table tb1")
	tk.MustQuery("explain format = 'brief' select * from tb1 where  id > 50 and id < 200").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:tb1 range:(50,200), keep order:false"))
}
