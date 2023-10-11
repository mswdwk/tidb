package hbase

import (
	"context"
	"fmt"

	"github.com/tsuna/gohbase/hrpc"
)

func displayCells(result *hrpc.Result) {
	for k, v := range result.Cells { // v结构体中的Value保存了真正的数据
		// value := v.Value
		fmt.Printf("key:" + string(k))
		//fmt.Printf("v=%V"+ *v)
		fmt.Printf("\tRow:" + string(v.Row))
		fmt.Printf("\tFamily:" + string(v.Family))
		fmt.Printf("\tQualifier:" + string(v.Qualifier))
		fmt.Printf("\tvalue:" + string(v.Value))
		fmt.Printf("\tcellType:" + string(*v.CellType))
		fmt.Println("\ttags:" + string(v.Tags))
		//      var myuser mystruct
		//      err := json.Unmarshal(value, &myuser) // value为 []unit8类型的字节数组，所以可以直接放到json.Unmarshal
		//      if err != nil {
		//              fmt.Println(err.Error())
		//      }
		//      fmt.Println(myuser)
	}
}

func GetOneRowkey(tablename string, rowkey string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	// hrpc.MaxVersions(3)
	var getRequest, err = hrpc.NewGetStr(context.Background(), tablename, rowkey, hrpc.MaxVersions(3))
	// getRequest.maxVersions = 3
	getRsp, err := G_HbaseClient.Get(getRequest) // Get()方法返回查询结果。通过客户端真正读取数据

	if err != nil {
		fmt.Println("hbase get client error:" + err.Error())
		return
	}
	fmt.Printf("get table %s rowkey %s\n", tablename, rowkey)
	displayCells(getRsp)
}

func PutOneRowOneFiled(tablename string, rowkey string, cf string, field string, value string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	values := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(value)}}
	var putRequest, err = hrpc.NewPutStr(context.Background(), tablename, rowkey, values)
	resp, err := G_HbaseClient.Put(putRequest)

	if err != nil {
		fmt.Println("hbase client put row error:" + err.Error())
		return
	}
	fmt.Printf("table %s put rowkey %s [%s:%s] value[%s] , resp partial %t\n",
		tablename, rowkey, cf, field, value, resp.Partial)
}

func PutOneRowOneCf(tablename string, rowkey string, cf string, field_values map[string][]byte) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	values := map[string]map[string][]byte{cf: field_values}
	var putRequest, err = hrpc.NewPutStr(context.Background(), tablename, rowkey, values)
	resp, err := G_HbaseClient.Put(putRequest)

	if err != nil {
		fmt.Println("hbase client put one row failed, table ", tablename, ",error "+err.Error())
		return
	}
	fmt.Printf("table %s put rowkey %s cf=%s field_values=%v , resp partial %t\n",
		tablename, rowkey, cf, field_values, resp.Partial)
}
func CheckAndPutOneRow(tablename string, rowkey string, cf string, field string, oldvalue string, newvalue string) {
	// oldValueMap := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(oldvalue)}}
	// oldValue, err := json.Marshal(oldValueMap)
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	newValueMap := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(newvalue)}}
	newRequest, _ := hrpc.NewPutStr(context.Background(), tablename, rowkey, newValueMap)

	ret, err := G_HbaseClient.CheckAndPut(newRequest, cf, field, []byte(oldvalue))

	if err != nil {
		fmt.Println("hbase client chaeck and put row error:" + err.Error())
		return
	}
	fmt.Printf("check and put table %s rowkey %s [%s:%s] oldV[%s] newV[%s], ret %t\n",
		tablename, rowkey, cf, field, oldvalue, newvalue, ret)
}

func DeleteOneRow(tablename string, rowkey string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	// values := map[string]map[string][]byte{"cf": {"a": []byte(time.Now().String())}}
	// values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	putRequest, _ := hrpc.NewDelStr(context.Background(), tablename, rowkey, nil)
	resp, err := G_HbaseClient.Delete(putRequest)

	if err != nil {
		fmt.Println("hbase client delete one row failed: rowkey=", rowkey, ",error="+err.Error())
		return
	}
	displayCells(resp)

	fmt.Println("delete row ok,tablename=", tablename, ",rowkey=", rowkey)
}

// create 'member','member_id','address','info'
type mystruct struct {
	Use    string               `json:"user_id" `
	Movies map[string][]float64 `json:"movies" ` // 用户看的多部电影 "电影id":[打分int,喜好程度float]
}
