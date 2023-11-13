package hbase

import (
	"fmt"
	"strings"

	"errors"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/tsuna/gohbase/hrpc"
)

func Cell2Map(h *hrpc.Result) map[string][]byte {
	if nil == h {
		return nil
	}

	m := make(map[string][]byte, 16)
	for i, v := range h.Cells { // v结构体中的Value保存了真正的数据
		// value := v.Value
		fmt.Printf("i: %d", i)
		//fmt.Printf("v=%V"+ *v)
		fmt.Printf("\tRow:" + string(v.Row))
		fmt.Printf("\tFamily:" + string(v.Family))
		fmt.Printf("\tQualifier:" + string(v.Qualifier))
		key := string(v.Qualifier)
		fmt.Println("\tvalue:" + string(v.Value))
		// fmt.Printf("\tcellType:" + string(*v.CellType))
		// fmt.Println("\ttags:" + string(v.Tags))
		m[key] = v.Value
		//      var myuser mystruct
		//      err := json.Unmarshal(value, &myuser) // value为 []unit8类型的字节数组，所以可以直接放到json.Unmarshal
		//      if err != nil {
		//              fmt.Println(err.Error())
		//      }
		//      fmt.Println(myuser)
	}
	return m
}

// reference: func DecodeRowValToChunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo,handle kv.Handle, rowVal []byte, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder)
//  			DecodeToChunk
// 				decodeColToChunk
// TODO:  Hbase multi version for one row/column
// HrpcResult2Chunk decodes *hrpc.Result value into chunk checking row format used.

func HrpcResult2Chunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo,
	handle kv.Handle, val *hrpc.Result, chk *chunk.Chunk, decoder *rowcodec.ChunkDecoder) error {
	hrMap := Cell2Map(val)
	if nil == hrMap {
		return errors.New("hrpc result is nil")
	}

	if len(hrMap) == 0 {
		fmt.Println("hbase get 0 result")
		return nil
	}
	// err := decoder.fromBytes(rowData)
	// if err != nil {
	// 	return err
	// }
	// var decoder rowcodec.ChunkDecoder = *rd //codec.NewDecoder(chk, sctx.GetSessionVars().Location())
	kvmap := make(map[int64][]byte, 16)
	for _, col := range schema.Columns {
		// colData := hrMap[col.OrigName]
		// Bug Fix:
		// 		INPUT SQL:	select * from tb1 where id = 1 or id = 1
		//		the col is like 'test.tb1.id' not 'id', then hrMap can not find it.
		if 0 == len(col.OrigName) {
			continue
		}
		colNames := strings.SplitAfter(col.OrigName, ".")
		colName := colNames[len(colNames)-1]
		if val, ok := hrMap[colName]; ok {
			kvmap[col.ID] = val
		} else {
			fmt.Println("Error: can not get column " + col.OrigName + " colName " + colName)
			break
		}
		fmt.Println("col.OrigName=", col.OrigName, "col.ID=", col.ID, "col.UniqueID=", col.UniqueID)
	}
	if 0 == len(kvmap) {
		fmt.Println("kvmap no data")
		return nil
	}
	// TODO check colData is not nil
	// err := decoder.DecodeColToChunk(col.Index, col, colData, chk)
	err := decoder.DecodeToChunk2(sctx.GetSessionVars().StmtCtx, kvmap, handle, chk)
	if err != nil {
		return err
	}

	return nil
}

/*{
	decoder := codec.NewDecoder(chk, sctx.GetSessionVars().Location())
	for i, col := range schema.Columns {
		// fill the virtual column value after row calculation
		if col.VirtualExpr != nil {
			chk.AppendNull(i)
			continue
		}
		// find value by column name from hrMap then set to chk
		// TODO:

		// convert value to datum accordding to column type
		// var d types.Datum
		// d.SetBytes(hrMap[col.OrigName])
		// chk.AppendDatum(i, &d)

		// rd.DecodeToChunk(hrMap[col.OrigName], handle, chk)
		colData := hrMap[col.OrigName]
		colIdx := col.Index
		err := decoder.decodeColToChunk(colIdx, col, colData, chk)
	}
	return nil
}*/
