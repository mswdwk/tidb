// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hbase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var G_HbaseClient gohbase.Client
var G_HbaseAdminClient gohbase.AdminClient

func GetHbaseClient() gohbase.Client {
	return G_HbaseClient
}

func GetHbaseAdminClient() gohbase.AdminClient {
	return G_HbaseAdminClient
}

func InitHbaseClient(path string) error {
	fmt.Printf("hbasedirver init path=%s, time %s\n", path, time.Now())
	G_HbaseClient = gohbase.NewClient(path)
	G_HbaseAdminClient = gohbase.NewAdminClient(path)
	v := map[string]map[string][]byte{
		"cf": map[string][]byte{
			"q1": []byte(time.Now().String()),
			"q2": nil,
		},
	}

	putRequest, err := hrpc.NewPutStr(context.Background(), "tidb", "row1234", v)
	_, err = G_HbaseClient.Put(putRequest)
	if err != nil {
		fmt.Println("hbase put failed: ", err)
	}

	fmt.Println("init hbase client ok!")
	return nil
}

// CreateTable creates the given table with the given families
func HbaseCreateTable(client gohbase.AdminClient, table string, cFamilies []string, maxVersion uint32) error {
	// If the table exists, delete it
	// DeleteTable(client, table)
	// Don't check the error, since one will be returned if the table doesn't
	// exist
	if nil == client {
		fmt.Println("hbaseAdminClient is nil !")
		return errors.New("hbaseAdminClient is nil !")
	}

	cf := make(map[string]map[string]string, len(cFamilies))
	for _, f := range cFamilies {
		cf[f] = nil
	}
	// TODO: optimize
	// pre-split table for reverse scan test of region changes
	keySplits := [][]byte{[]byte("REVTEST-100"), []byte("REVTEST-200"), []byte("REVTEST-300")}
	hrpc.MaxVersions(maxVersion)
	ct := hrpc.NewCreateTable(context.Background(), []byte(table), cf, hrpc.SplitKeys(keySplits))
	if err := client.CreateTable(ct); err != nil {
		return err
	}

	return nil
}

// DeleteTable finds the HBase shell via the HBASE_HOME environment variable,
// and disables and drops the given table
func DeleteTable(client gohbase.AdminClient, table string) error {
	if nil == client {
		fmt.Println("hbaseAdminClient is nil !")
		return errors.New("hbaseAdminClient is nil !")
	}

	dit := hrpc.NewDisableTable(context.Background(), []byte(table))
	err := client.DisableTable(dit)
	if err != nil {
		if !strings.Contains(err.Error(), "TableNotEnabledException") {
			return err
		}
	}

	det := hrpc.NewDeleteTable(context.Background(), []byte(table))
	err = client.DeleteTable(det)
	if err != nil {
		return err
	}
	return nil
}
