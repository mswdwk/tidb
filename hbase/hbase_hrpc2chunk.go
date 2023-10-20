package hbase

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
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
		fmt.Printf("i:" + string(i))
		//fmt.Printf("v=%V"+ *v)
		fmt.Printf("\tRow:" + string(v.Row))
		fmt.Printf("\tFamily:" + string(v.Family))
		fmt.Printf("\tQualifier:" + string(v.Qualifier))
		key := string(v.Qualifier)
		fmt.Printf("\tvalue:" + string(v.Value))
		fmt.Printf("\tcellType:" + string(*v.CellType))
		fmt.Println("\ttags:" + string(v.Tags))
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

// TODO:  Hbase multi version for one row/column
// HrpcResult2Chunk decodes *hrpc.Result value into chunk checking row format used.

func HrpcResult2Chunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo,
	handle kv.Handle, val *hrpc.Result, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder) error {
	hrMap := Cell2Map(val)
	// decoder := codec.NewDecoder(chk, sctx.GetSessionVars().Location())
	for i, col := range schema.Columns {
		// fill the virtual column value after row calculation
		if col.VirtualExpr != nil {
			chk.AppendNull(i)
			continue
		}
		// find value by column name from hrMap then set to chk
		// TODO:
		var d types.Datum
		//convert value to datum accordding to column type
		d.SetBytes(hrMap[col.OrigName])
		chk.AppendDatum(i, &d)
		// rd.DecodeToChunk(hrMap[col.OrigName], handle, chk)

	}
	return nil
}
