package hbase

import (
	"encoding/binary"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pkg/errors"
)

func EncodeUint(buf []byte, uVal uint64) []byte {
	return codec.EncodeUint(buf, uVal)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uVal)
	buf = append(buf, tmp[:8]...)
	return buf
}

func EncodeInt(buf []byte, uVal int64) []byte {
	return codec.EncodeInt(buf, uVal)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(uVal))
	buf = append(buf, tmp[:8]...)
	return buf
}

// encodeValueDatum encodes one row datum entry into bytes.
// due to encode as value, this method will flatten value type like tablecodec.flatten
func EncodeValueDatum(sc *stmtctx.StatementContext, d *types.Datum, buffer []byte) (nBuffer []byte, err error) {
	switch d.Kind() {
	case types.KindInt64:
		buffer = EncodeInt(buffer, d.GetInt64())
	case types.KindUint64:
		buffer = EncodeUint(buffer, d.GetUint64())
	case types.KindString, types.KindBytes:
		buffer = append(buffer, d.GetBytes()...)
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := d.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && sc != nil && sc.TimeZone != time.UTC {
			err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return
			}
		}
		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		buffer = EncodeUint(buffer, v)
	case types.KindMysqlDuration:
		buffer = EncodeInt(buffer, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlEnum:
		buffer = EncodeUint(buffer, d.GetMysqlEnum().Value)
	case types.KindMysqlSet:
		buffer = EncodeUint(buffer, d.GetMysqlSet().Value)
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		var val uint64
		val, err = d.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return
		}
		buffer = EncodeUint(buffer, val)
	case types.KindFloat32, types.KindFloat64:
		buffer = codec.EncodeFloat(buffer, d.GetFloat64())
	case types.KindMysqlDecimal:
		buffer, err = codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil && sc != nil {
			if terror.ErrorEqual(err, types.ErrTruncated) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		}
	case types.KindMysqlJSON:
		j := d.GetMysqlJSON()
		buffer = append(buffer, j.TypeCode)
		buffer = append(buffer, j.Value...)
	default:
		err = errors.Errorf("unsupport encode type %d", d.Kind())
	}
	nBuffer = buffer
	return
}
