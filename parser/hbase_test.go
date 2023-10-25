package parser_test

import "testing"

func TestHbaseDDL(t *testing.T) {
	table := []testCase{
		// create hbase table
		{"create table t (created_at datetime) data_source=hbase", true, "CREATE TABLE `t` (`created_at` DATETIME) DATA_SOURCE = 'hbase'"},
		{"create table t (created_at datetime) table_mapping=1", true, "CREATE TABLE `t` (`created_at` DATETIME) TABLE_MAPPING = true"},
		{"create table t (created_at datetime) data_source=hbase,table_mapping=1", true, "CREATE TABLE `t` (`created_at` DATETIME) DATA_SOURCE = 'hbase' TABLE_MAPPING = true"},
		{"create table t (created_at datetime) table_mapping=0 data_source= hbase", true, "CREATE TABLE `t` (`created_at` DATETIME) TABLE_MAPPING = false DATA_SOURCE = 'hbase'"},
		{"create table t (created_at datetime) data_source hbase,table_mapping false", true, "CREATE TABLE `t` (`created_at` DATETIME) DATA_SOURCE = 'hbase' TABLE_MAPPING = false"},
		{"create table t (created_at datetime) data_source hbase,table_mapping=true", true, "CREATE TABLE `t` (`created_at` DATETIME) DATA_SOURCE = 'hbase' TABLE_MAPPING = true"},
	}

	RunTest(t, table, false)
}
