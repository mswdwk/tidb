package hbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/tsuna/gohbase/hrpc"
)

func displayCells(result *hrpc.Result) {
	for i, v := range result.Cells { // v结构体中的Value保存了真正的数据
		// value := v.Value
		fmt.Printf("i: %d", i)
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

func GetOneRowkey(tableName string, rowkey string) (*hrpc.Result, error) {
	if nil == G_HbaseClient {
		err := errors.New("hbase cleint is nil")
		fmt.Println("error: hbase client is nil")
		return nil, err
	}
	// hrpc.MaxVersions(3)
	var getRequest, err = hrpc.NewGetStr(context.Background(), tableName, rowkey, hrpc.MaxVersions(3))
	// getRequest.maxVersions = 3
	getRsp, err := G_HbaseClient.Get(getRequest) // Get()方法返回查询结果。通过客户端真正读取数据

	if err != nil {
		fmt.Println("hbase client get rowkey error:"+err.Error(), ",tableName ", tableName)
		return nil, err
	}
	// logutil.BgLogger().Debug(fmt.Sprintf("get hbase table %s rowkey %s\n", tableName, rowkey))
	// displayCells(getRsp)
	return getRsp, nil
}

func PutOneRowOneFiled(tableName string, rowkey string, cf string, field string, value string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	values := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(value)}}
	var putRequest, err = hrpc.NewPutStr(context.Background(), tableName, rowkey, values)
	resp, err := G_HbaseClient.Put(putRequest)

	if err != nil {
		fmt.Println("hbase client put row error:" + err.Error() + ",resp= " + resp.String())
		return
	}

	// logutil.BgLogger().Debug(fmt.Sprint("table %s put rowkey %s [%s:%s] value[%s] , resp partial %t\n", tableName, rowkey, cf, field, value, resp.Partial))
}

func PutOneRowOneCf(tableName string, rowkey string, cf string, field_values map[string][]byte) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	values := map[string]map[string][]byte{cf: field_values}
	var putRequest, err = hrpc.NewPutStr(context.Background(), tableName, rowkey, values)
	resp, err := G_HbaseClient.Put(putRequest)

	if err != nil {
		fmt.Println("hbase client put one row failed, table ", tableName, ",error "+err.Error()+", resp="+resp.String())
		return
	}
	//logutil.BgLogger().Debug(fmt.Sprintf("table %s put rowkey %s cf=%s field_values=%v , resp partial %t\n",tableName, rowkey, cf, field_values, resp.Partial))
}

func CheckAndPutOneRow(tableName string, rowkey string, cf string, field string, oldvalue string, newvalue string) {
	// oldValueMap := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(oldvalue)}}
	// oldValue, err := json.Marshal(oldValueMap)
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	newValueMap := map[string]map[string][]byte{cf: map[string][]byte{field: []byte(newvalue)}}
	newRequest, _ := hrpc.NewPutStr(context.Background(), tableName, rowkey, newValueMap)

	resp, err := G_HbaseClient.CheckAndPut(newRequest, cf, field, []byte(oldvalue))

	if err != nil {
		fmt.Printf("hbase client chaeck and put row error: %s resp= %t\n", err.Error(), resp)
		return
	}
	// logutil.BgLogger().Debug(fmt.Sprintf("check and put table %s rowkey %s [%s:%s] oldV[%s] newV[%s], ret %t\n",tableName, rowkey, cf, field, oldvalue, newvalue, ret))
}

func DeleteOneRow(tableName string, rowkey string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	// values := map[string]map[string][]byte{"cf": {"a": []byte(time.Now().String())}}
	// values := map[string]map[string][]byte{"cf": map[string][]byte{"a": []byte("1")}}
	putRequest, _ := hrpc.NewDelStr(context.Background(), tableName, rowkey, nil)
	_, err := G_HbaseClient.Delete(putRequest)

	if err != nil {
		fmt.Println("hbase client delete one row failed: rowkey=", rowkey, ",error="+err.Error())
		return
	}
	//displayCells(resp)

	// logutil.BgLogger().Debug(fmt.Sprintln("delete row ok,tableName=", tableName, ",rowkey=", rowkey))
}

func TableScanRange(tableName string, startRow, stopRow string) {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return
	}
	fmt.Printf("scan hbase table %s startRow %s stopRow %s\n", tableName, startRow, stopRow)
	// hrpc.MaxVersions(3)
	request, err := hrpc.NewScanRange(context.Background(), []byte(tableName), []byte(startRow), []byte(stopRow))
	if err != nil {
		fmt.Println("hbase get scanrange error:" + err.Error())
		return
	}
	// getRequest.maxVersions = 3
	scan := G_HbaseClient.Scan(request) // Scan()方法返回查询结果。通过客户端真正读取数据
	for {
		r, err := scan.Next()
		if nil != err {
			fmt.Println("finish scan table " + tableName + " , err " + err.Error())
			break
		}
		displayCells(r)
	}
}

func TableScanRangeOpen(tableName string, startRow, stopRow string) *hrpc.Scanner {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return nil
	}
	// hrpc.MaxVersions(3)
	request, err := hrpc.NewScanRange(context.Background(), []byte(tableName), []byte(startRow), []byte(stopRow))
	if err != nil {
		fmt.Println("hbase get client error:" + err.Error())
		return nil
	}
	// getRequest.maxVersions = 3
	scan := G_HbaseClient.Scan(request) // Scan()方法返回查询结果。通过客户端真正读取数据
	// defer scan.Close()
	if scan == nil {
		fmt.Println("hbase get client error:" + err.Error())
		return nil
	}
	// logutil.BgLogger().Debug(fmt.Sprintf("OPEN: scan hbase table %s startRow %s stopRow %s\n", tableName, startRow, stopRow))
	return &scan
}

// TODO HBASE: ADD FILTER, LIKE TestNewScan

func TableScanRangeNext(tableName string, startRow, stopRow string, scan *hrpc.Scanner) *hrpc.Result {
	if nil == G_HbaseClient {
		fmt.Println("error: hbase client is nil")
		return nil
	}

	// logutil.BgLogger().Debug(fmt.Sprintf("NEXT: scan hbase table %s startRow %s stopRow %s\n", tableName, startRow, stopRow))

	r, err := (*scan).Next()
	if nil != err || nil == r || (nil != r && 0 == len(r.Cells)) {
		fmt.Println("finish scan hbase table " + tableName + " , err " + err.Error())
		return nil
	}
	return r
}

func TableScanRangeClose(tableName string, startRow, stopRow string, scan *hrpc.Scanner) error {
	if nil == scan {
		fmt.Println("error: hbase scan is nil")
		return errors.New("hbase scan is nil ")
	}
	// logutil.BgLogger().Debug(fmt.Sprintf("CLOSE: scan hbase table %s startRow %s stopRow %s\n", tableName, startRow, stopRow))

	err := (*scan).Close()
	return err
}
