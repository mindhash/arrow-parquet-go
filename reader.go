/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/minio/parquet-go/gen-go/parquet"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
)

// GetReaderFunc - function type returning io.ReadCloser for requested offset/length.
type GetReaderFunc func(offset, length int64) (io.ReadCloser, error)

func footerSize(getReaderFunc GetReaderFunc) (size int64, err error) {
	rc, err := getReaderFunc(-8, 4)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	buf := make([]byte, 4)
	if _, err = io.ReadFull(rc, buf); err != nil {
		return 0, err
	}

	size = int64(binary.LittleEndian.Uint32(buf))

	return size, nil
}

func fileMetadata(getReaderFunc GetReaderFunc) (*parquet.FileMetaData, error) {
	size, err := footerSize(getReaderFunc)
	if err != nil {
		return nil, err
	}

	rc, err := getReaderFunc(-(8 + size), size)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	fileMeta := parquet.NewFileMetaData()

	pf := thrift.NewTCompactProtocolFactory()
	protocol := pf.GetProtocol(thrift.NewStreamTransportR(rc))
	err = fileMeta.Read(protocol)
	if err != nil {
		return nil, err
	}

	return fileMeta, nil
}

func toArrowType(parquetType parquet.Type) (dataType arrow.DataType, err error) {

	// TODO: add more types
	// String type
	// parquet.Type_INT96,  parquet.Type_FIXED_LEN_BYTE_ARRAY

	switch parquetType {
	case parquet.Type_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case parquet.Type_INT32:
		return arrow.PrimitiveTypes.Int32, nil
	case parquet.Type_INT64:
		return arrow.PrimitiveTypes.Int64, nil
	case parquet.Type_FLOAT, parquet.Type_DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil

	case parquet.Type_BYTE_ARRAY:
		return arrow.BinaryTypes.Binary, nil

	default:
		return nil, fmt.Errorf("Failed to parse Parquet Type into Arrow Data type: %v", parquetType.String())
	}
}

func toArrowField(colmeta *parquet.ColumnMetaData) (arrow.Field, error) {

	var name string
	if len(colmeta.PathInSchema) > 1 {
		name = strings.Join(colmeta.PathInSchema[:], ".")
	} else {
		name = colmeta.PathInSchema[0]
	}

	ft, err := toArrowType(colmeta.Type)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{Name: name, Type: ft}, nil
}

func toArrowCol(mem memory.Allocator, values []interface{}, arrowType arrow.DataType, ignoreErrors bool) (array.Interface, error) {

	// todo : if arrow can have append interface that type casts
	switch arrowType {
	case arrow.PrimitiveTypes.Float64:
		fb64 := array.NewFloat64Builder(mem)
		for _, val := range values {
			if val == nil {
				fb64.AppendNull()
				continue
			}

			val, ok := val.(float64)
			if !ok {
				if ignoreErrors {
					fb64.AppendNull()
					continue
				}
				return nil, fmt.Errorf("Failed to convert value to %v", arrowType)
			}

			fb64.Append(val)
		}
		return fb64.NewArray(), nil

	case arrow.PrimitiveTypes.Int64:
		i64 := array.NewInt64Builder(mem)
		for _, val := range values {
			if val == nil {
				i64.AppendNull()
				continue
			}

			val, ok := val.(int64)
			if !ok {
				if ignoreErrors {
					i64.AppendNull()
					continue
				}
				return nil, fmt.Errorf("Failed to convert value to %v", arrowType)
			}

			i64.Append(val)
		}
		return i64.NewArray(), nil
	case arrow.BinaryTypes.String:
		bib := array.NewStringBuilder(mem)
		for _, val := range values {
			if val == nil {
				bib.AppendNull()
				continue
			}

			val, ok := val.(string)
			if !ok {
				if ignoreErrors {
					bib.AppendNull()
					continue
				}
				return nil, fmt.Errorf("Failed to convert value to %v", arrowType)
			}

			bib.Append(val)
		}
		return bib.NewArray(), nil
	case arrow.BinaryTypes.Binary:
		bib := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		for _, val := range values {
			if val == nil {
				bib.AppendNull()
				continue
			}

			val, ok := val.([]byte)
			if !ok {
				if ignoreErrors {
					bib.AppendNull()
					continue
				}
				return nil, fmt.Errorf("Failed to convert value to %v", arrowType)
			}

			bib.Append(val)
		}
		return bib.NewArray(), nil
	case arrow.FixedWidthTypes.Boolean:
		bb := array.NewBooleanBuilder(mem)
		for _, val := range values {
			if val == nil {
				bb.AppendNull()
				continue
			}

			val, ok := val.(bool)
			if !ok {
				if ignoreErrors {
					bb.AppendNull()
					continue
				}
				return nil, fmt.Errorf("Failed to convert value to %v", arrowType)
			}

			bb.Append(val)
		}
		return bb.NewArray(), nil
	default:
		return nil, fmt.Errorf("Unimplemented arrow data type found: %v", arrowType)
	}

}

// Value - denotes column value
// type Value struct {
// 	Value interface{}
// 	Type  parquet.Type
// }

// MarshalJSON - encodes to JSON data
// func (value Value) MarshalJSON() (data []byte, err error) {
// 	return json.Marshal(value.Value)
// }

// Reader - denotes parquet file.
type Reader struct {
	getReaderFunc  GetReaderFunc
	schemaElements []*parquet.SchemaElement
	rowGroups      []*parquet.RowGroup
	rowGroupIndex  int

	nameList []string
	columns  map[string]*column
	rowIndex int64

	// fieldset and names are used to generate arrow schema
	fieldset      []arrow.Field
	fieldsetnames map[string]bool
	mem           memory.Allocator
}

// NewReader - creates new parquet reader. Reader calls getReaderFunc to handle on target file
func NewReader(getReaderFunc GetReaderFunc) (*Reader, error) {
	fileMeta, err := fileMetadata(getReaderFunc)
	if err != nil {
		return nil, err
	}

	nameList := []string{}
	schemaElements := fileMeta.GetSchema()
	for _, element := range schemaElements {
		nameList = append(nameList, element.Name)
	}
	// fix: need a better way to manage length
	// name list also has parent name elements of hierarchical record
	// so not able to use len(nameList)
	// for example: schema { column: 3} will have 2 entries in nameList
	// {schema, column} but the final field that we want is just column
	// need a better way to read through parquet column metadata
	fieldset := make([]arrow.Field, 0)
	fieldsetnames := make(map[string]bool)

	return &Reader{
		getReaderFunc:  getReaderFunc,
		rowGroups:      fileMeta.GetRowGroups(),
		schemaElements: schemaElements,
		nameList:       nameList,
		mem:            memory.DefaultAllocator,
		fieldset:       fieldset,
		fieldsetnames:  fieldsetnames,
	}, nil
}

// Read - reads single record batch.
func (reader *Reader) Read() (record array.Record, err error) {
	if reader.rowGroupIndex >= len(reader.rowGroups) {
		return nil, io.EOF
	}

	if reader.columns == nil {
		reader.columns, err = getColumns(
			reader.rowGroups[reader.rowGroupIndex],
			reader.schemaElements,
			reader.getReaderFunc,
		)
		if err != nil {
			return nil, err
		}

		reader.rowIndex = 0
	}

	if reader.rowIndex >= reader.rowGroups[reader.rowGroupIndex].GetNumRows() {
		reader.rowGroupIndex++
		reader.Close()
		return reader.Read()
	}

	cols := make([]array.Interface, 0)

	readCount := 0
	for name := range reader.columns {
		if !reader.fieldsetnames[name] {
			field, fielderr := toArrowField(reader.columns[name].metadata)
			if fielderr != nil {
				panic(fielderr)
			}
			reader.fieldset = append(reader.fieldset, field)
		}

		reader.fieldsetnames[name] = true
		values, parquetType := reader.columns[name].read()
		if readCount == 0 {
			readCount = len(values)
		}

		valArrowType, _ := toArrowType(parquetType)

		col, err := toArrowCol(reader.mem, values, valArrowType, true)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}

	// create new record batch
	record = newRecord(reader.fieldset, cols)
	reader.rowIndex = reader.rowIndex + int64(readCount)

	return record, nil
}

func newRecord(fieldset []arrow.Field, cols []array.Interface) array.Record {
	schema := arrow.NewSchema(fieldset, nil)
	// TODO: reuse record  from mem allocator
	rec := array.NewRecord(schema, cols, -1)
	return rec
}

// Close - closes underneath readers.
func (reader *Reader) Close() (err error) {

	for _, column := range reader.columns {
		column.close()
	}

	reader.columns = nil
	reader.rowIndex = 0

	return nil
}
