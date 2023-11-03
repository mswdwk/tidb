package hbase_test

import (
	"testing"

	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// reference TestCrossValidationSelectivity  TestFlatPhysicalPlan

// keyword : TypeTableRangeScan  typeTableRangeScanID  PhysicalTableScan  buildSelect BuildDataSourceFromView buildProjUponView

/*
	view   meta : ViewInfo
	added field : SelectStmt2 string
				  Expr 		  string
		   func : BuildViewInfo
*/

// show create view v1  -> fetchShowCreateTable4View

/*
	hbase table hb1 (dataSource s1):   id >  1 and id < 100
	tikv  table tb1 (dataSource s2):   id >=100
	hyper view   v1 :
		create view v1 if  id >=120
			then
				select * from hb1
			else
				select * from tb1
			end

	INPUT SQL:
		select * from v1 where id > 50 and id < 200

	dataSource caculate:
		selections [ "id > 50" , "id < 200" ]
		                 |           |
					     V           V
				      [s1,s2]     [s1,s2]
						     \   /
						       |
				               V
	                  tablerefs:[s1,s2]
	PLAN:
		number: 2
		plan 1 (datasource s1):
				select * from hb1 where id > 50 and id < 200
			TableReader
				TableRangeScan

		plan 2 (datasource s2):
				select * from tb1 where id > 50 and id < 200
			TableReader
				TableRangeScan
*/

func TestCreateView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("create table hb1 (id int primary key, name varchar(64))")
	tk.MustExec("create table tb1 (id int primary key, name varchar(64))")
	tk.MustExec("create view v1 if id >= 100 then select * from hb1 else select * from tb1")
	tk.MustExec("insert into tb1 values (10,'name 10'),(30,'name 30'),(60,'name 60'),(90,'name 90')")
	tk.MustExec("insert into hb1 values (100,'name 100'), (150,'name 150'),(200,'name 200'),(300,'name 300')")
}

func TestSelection(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists hb1")
	tk.MustExec("drop table if exists tb1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("create table hb1 (id int primary key, name varchar(64))")
	tk.MustExec("create table tb1 (id int primary key, name varchar(64))")
	tk.MustExec("create view v1 if id >= 100 then select * from hb1 else select * from tb1")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("insert into tb1 values (10,'name 10'),(30,'name 30'),(60,'name 60'),(90,'name 90')")
	tk.MustExec("insert into hb1 values (100,'name 100'),(150,'name 150'),(200,'name 200'),(300,'name 300')")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table hb1")
	tk.MustExec("analyze table tb1")
	tk.MustQuery("explain format = 'brief' select * from tb1 where  id > 50 and id < 200").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:tb1 range:(50,200), keep order:false"))
}

/* 1 DataSource
'explain select * from v1 where id > 50 and id < 200'  should output plan like below

mysql> explain format='brief' select tb1.id,tb1.name from tb1 where id > 50 and id < 200;
+----------------------+---------+-----------+---------------+----------------------------------+
| id                   | estRows | task      | access object | operator info                    |
+----------------------+---------+-----------+---------------+----------------------------------+
| TableReader          | 6.00    | root      |               | data:TableRangeScan              |
| └─TableRangeScan     | 6.00    | cop[tikv] | table:tb1     | range:(50,200), keep order:false |
+----------------------+---------+-----------+---------------+----------------------------------+
2 rows in set (0.01 sec)

mysql> explain format='brief' select tb1.id,tb1.name from tb1 where  tb1.id < 70;
+----------------------+---------+-----------+---------------+-----------------------------------+
| id                   | estRows | task      | access object | operator info                     |
+----------------------+---------+-----------+---------------+-----------------------------------+
| TableReader          | 8.00    | root      |               | data:TableRangeScan               |
| └─TableRangeScan     | 8.00    | cop[tikv] | table:tb1     | range:[-inf,70), keep order:false |
+----------------------+---------+-----------+---------------+-----------------------------------+
2 rows in set (0.00 sec)

*/

/*   2 DataSource
'explain select * from v1 where id > 50 and id < 200'  should output plan like below

mysql> explain format='brief' select tb1.id,tb1.name from tb1 where  tb1.id > 50 and tb1.id < 200 union select hb1.id,hb1.name from hb1 where hb1.id > 50 and hb1.id < 200 ;
+----------------------------+---------+-----------+---------------+-----------------------------------------------------------------------------------------------------+
| id                         | estRows | task      | access object | operator info                                                                                       |
+----------------------------+---------+-----------+---------------+-----------------------------------------------------------------------------------------------------+
| HashAgg                    | 10.79   | root      |               | group by:Column#5, Column#6, funcs:firstrow(Column#5)->Column#5, funcs:firstrow(Column#6)->Column#6 |
| └─Union                    | 10.79   | root      |               |                                                                                                     |
|   ├─TableReader            | 6.00    | root      |               | data:TableRangeScan                                                                                 |
|   │ └─TableRangeScan       | 6.00    | cop[tikv] | table:tb1     | range:(50,200), keep order:false                                                                    |
|   └─TableReader            | 4.79    | root      |               | data:TableRangeScan                                                                                 |
|     └─TableRangeScan       | 4.79    | cop[tikv] | table:hb1     | range:(50,200), keep order:false                                                                    |
+----------------------------+---------+-----------+---------------+-----------------------------------------------------------------------------------------------------+
6 rows in set (0.00 sec)
*/
