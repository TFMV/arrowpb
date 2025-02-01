package arrowpb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ----------------------------------------------------------------------------
// 1) Descriptor Generation
// ----------------------------------------------------------------------------

// TypeMappings maps primitive Arrow types to their corresponding
// Protocol Buffers FieldDescriptorProto_Type. Extend for more coverage!
var TypeMappings = map[arrow.Type]descriptorpb.FieldDescriptorProto_Type{
	arrow.BOOL:    descriptorpb.FieldDescriptorProto_TYPE_BOOL,
	arrow.INT64:   descriptorpb.FieldDescriptorProto_TYPE_INT64,
	arrow.FLOAT64: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
	arrow.STRING:  descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.BINARY:  descriptorpb.FieldDescriptorProto_TYPE_BYTES,

	// Many Arrow date/time/timestamp types can be mapped to strings or
	// google.protobuf.Timestamp. For simplicity, we'll map them to strings:
	arrow.DATE32:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.DATE64:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.TIMESTAMP: descriptorpb.FieldDescriptorProto_TYPE_STRING,
}

// GenerateUniqueMessageName provides a unique name for the top-level message.
// In production, you might prefer a deterministic name or a different scheme.
func GenerateUniqueMessageName(prefix string) string {
	return fmt.Sprintf("%s_%d_%08d", prefix, time.Now().UnixNano(), rand.Int31())
}

// buildFieldDescriptor builds a FieldDescriptorProto for a single Arrow field.
func buildFieldDescriptor(field arrow.Field, index int32) (*descriptorpb.FieldDescriptorProto, []*descriptorpb.DescriptorProto, error) {
	fd := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(field.Name),
		Number: proto.Int32(index),
	}

	nestedMessages := []*descriptorpb.DescriptorProto{}

	// Handle nested StructTypes
	if st, ok := field.Type.(*arrow.StructType); ok {
		// Build a nested descriptor
		nestedName := fmt.Sprintf("%s_struct_%d", field.Name, index)
		nestedDesc, err := arrowStructToDescriptor(st, nestedName)
		if err != nil {
			return nil, nil, err
		}

		// The field will be a TYPE_MESSAGE referencing the nested descriptor
		fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
		fd.TypeName = proto.String(nestedName)
		nestedMessages = append(nestedMessages, nestedDesc)
		return fd, nestedMessages, nil
	}

	// Handle list (repeated) types, e.g. arrow.ListType or arrow.FixedSizeListType
	if lt, ok := field.Type.(*arrow.ListType); ok {
		// We only handle one-dimensional list -> repeated of the element type
		elemField := arrow.Field{
			Name: field.Name + "_elem",
			Type: lt.Elem(),
		}
		// We'll build a descriptor for the "inner" part
		// but effectively we'll mark the outer field as repeated
		innerFD, nested, err := buildFieldDescriptor(elemField, index)
		if err != nil {
			return nil, nil, err
		}
		// Force repeated
		fd.Label = descriptorpb.FieldDescriptorProto_Label(descriptorpb.FieldDescriptorProto_LABEL_REPEATED).Enum()

		// If it's a primitive element, just copy its type
		fd.Type = innerFD.Type
		fd.TypeName = proto.String(innerFD.GetTypeName())
		nestedMessages = append(nestedMessages, nested...)
		return fd, nestedMessages, nil
	}

	// If it's a known primitive (or string, bytes, etc.)
	t, ok := TypeMappings[field.Type.ID()]
	if !ok {
		return nil, nil, fmt.Errorf("unsupported Arrow type: %v", field.Type)
	}
	fd.Type = t.Enum()
	return fd, nil, nil
}

// arrowStructToDescriptor recursively builds a DescriptorProto from an Arrow StructType.
func arrowStructToDescriptor(st *arrow.StructType, messageName string) (*descriptorpb.DescriptorProto, error) {
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String(messageName),
	}

	var nestedTypes []*descriptorpb.DescriptorProto
	for i, f := range st.Fields() {
		fd, moreNested, err := buildFieldDescriptor(f, int32(i+1))
		if err != nil {
			return nil, err
		}
		desc.Field = append(desc.Field, fd)
		nestedTypes = append(nestedTypes, moreNested...)
	}

	// Add any nested descriptors to NestedType
	desc.NestedType = append(desc.NestedType, nestedTypes...)
	return desc, nil
}

// ArrowSchemaToFileDescriptorProto converts an Arrow schema into a FileDescriptorProto.
// This yields a top-level message plus any nested messages.
func ArrowSchemaToFileDescriptorProto(schema *arrow.Schema, packageName, messagePrefix string) (*descriptorpb.FileDescriptorProto, error) {
	// Build descriptor for top-level
	topMsgName := GenerateUniqueMessageName(messagePrefix)
	topDesc := &descriptorpb.DescriptorProto{
		Name: proto.String(topMsgName),
	}

	var nestedTypes []*descriptorpb.DescriptorProto

	// Build each field in the schema
	for i, field := range schema.Fields() {
		fd, moreNested, err := buildFieldDescriptor(field, int32(i+1))
		if err != nil {
			return nil, err
		}
		topDesc.Field = append(topDesc.Field, fd)
		nestedTypes = append(nestedTypes, moreNested...)
	}

	// Attach nested descriptors
	topDesc.NestedType = append(topDesc.NestedType, nestedTypes...)

	// Build a FileDescriptorProto
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String(topMsgName + ".proto"),
		Package: proto.String(packageName),
		MessageType: []*descriptorpb.DescriptorProto{
			topDesc,
		},
		Syntax: proto.String("proto3"),
	}
	return fdp, nil
}

// CompileFileDescriptorProto compiles a FileDescriptorProto into a protoreflect.FileDescriptor,
// which we can then use to obtain the top-level message descriptor for dynamic creation.
func CompileFileDescriptorProto(fdp *descriptorpb.FileDescriptorProto) (protoreflect.FileDescriptor, error) {
	// Create a FileDescriptorSet containing our single FileDescriptorProto
	fdset := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fdp},
	}
	files, err := protodesc.NewFiles(fdset)
	if err != nil {
		return nil, fmt.Errorf("failed to build file descriptors: %w", err)
	}
	fd, err := files.FindFileByPath(fdp.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to find file descriptor by path: %w", err)
	}
	return fd, nil
}

// GetTopLevelMessageDescriptor retrieves the first (top-level) message descriptor from the compiled file.
func GetTopLevelMessageDescriptor(fd protoreflect.FileDescriptor) (protoreflect.MessageDescriptor, error) {
	if fd.Messages().Len() == 0 {
		return nil, errors.New("file descriptor has no top-level messages")
	}
	return fd.Messages().Get(0), nil
}

// ----------------------------------------------------------------------------
// 2) Converting Arrow to Protobuf via Dynamic Messages
// ----------------------------------------------------------------------------

// setDynamicField sets a single field in a dynamic message, given a value from an Arrow array.
// This function is carefully written to handle types without reflection overhead.
func setDynamicField(msg *dynamicpb.Message, fieldDesc protoreflect.FieldDescriptor, val interface{}) {
	if val == nil {
		// For proto3, "unset" simply means default. If you need
		// to differentiate null vs default, you'll need proto2 or WKT wrappers.
		return
	}

	// If the field is repeated, we expect 'val' to be a slice. In this example,
	// we handle it up the chain, so let's assume no repeated here unless
	// you want to expand for lists.
	switch fieldDesc.Kind() {
	case protoreflect.BoolKind:
		if b, ok := val.(bool); ok {
			msg.Set(fieldDesc, protoreflect.ValueOfBool(b))
		}
	case protoreflect.Int64Kind:
		if i, ok := val.(int64); ok {
			msg.Set(fieldDesc, protoreflect.ValueOfInt64(i))
		}
	case protoreflect.DoubleKind:
		if f, ok := val.(float64); ok {
			msg.Set(fieldDesc, protoreflect.ValueOfFloat64(f))
		}
	case protoreflect.StringKind:
		if s, ok := val.(string); ok {
			msg.Set(fieldDesc, protoreflect.ValueOfString(s))
		}
	case protoreflect.BytesKind:
		if b, ok := val.([]byte); ok {
			msg.Set(fieldDesc, protoreflect.ValueOfBytes(b))
		}
	case protoreflect.MessageKind:
		// This is a nested struct or repeated structure. If we have a map of sub-fields,
		// recursively set them. That means `val` should be another dynamic message or
		// a map. For brevity, see handleNestedStruct below.
		// If you have advanced needs (lists of messages, etc.), you'll need to expand it.
	default:
		// For protoreflect.Kind that we haven't handled (like enum, fixed32, etc.)
	}
}

// ExtractArrowValue is a helper that extracts the value at [rowIndex] from an Arrow array.
// Returns `nil` if the value is NULL or an unsupported type.
func ExtractArrowValue(col arrow.Array, rowIndex int) interface{} {
	if col.IsNull(rowIndex) {
		return nil
	}

	switch arr := col.(type) {
	// Boolean
	case *array.Boolean:
		return arr.Value(rowIndex)

	// Numeric types
	case *array.Int8:
		return int64(arr.Value(rowIndex))
	case *array.Int16:
		return int64(arr.Value(rowIndex))
	case *array.Int32:
		return int64(arr.Value(rowIndex))
	case *array.Int64:
		return arr.Value(rowIndex)
	case *array.Uint8:
		return int64(arr.Value(rowIndex))
	case *array.Uint16:
		return int64(arr.Value(rowIndex))
	case *array.Uint32:
		return int64(arr.Value(rowIndex))
	case *array.Uint64:
		return int64(arr.Value(rowIndex))
	case *array.Float32:
		return float64(arr.Value(rowIndex))
	case *array.Float64:
		return arr.Value(rowIndex)

	// String types
	case *array.String:
		return arr.Value(rowIndex)
	case *array.LargeString:
		return arr.Value(rowIndex)

	// Binary types
	case *array.Binary:
		return arr.Value(rowIndex)
	case *array.LargeBinary:
		return arr.Value(rowIndex)

	// Date/Time types
	case *array.Date32:
		return arr.Value(rowIndex).ToTime().Format(time.RFC3339)
	case *array.Date64:
		return arr.Value(rowIndex).ToTime().Format(time.RFC3339)
	case *array.Time32:
		return fmt.Sprintf("%v", arr.Value(rowIndex))
	case *array.Time64:
		return fmt.Sprintf("%v", arr.Value(rowIndex))
	case *array.Timestamp:
		return arr.Value(rowIndex).ToTime(arrow.Microsecond).Format(time.RFC3339)
	case *array.Duration:
		return fmt.Sprintf("%v", arr.Value(rowIndex))

	// Decimal types
	case *array.Decimal128:
		return fmt.Sprintf("%v", arr.Value(rowIndex))
	case *array.Decimal256:
		return fmt.Sprintf("%v", arr.Value(rowIndex))

	// Struct type (handled separately in handleNestedStruct)
	case *array.Struct:
		return nil

	// List types (would need special handling)
	case *array.List:
		return nil
	case *array.LargeList:
		return nil
	case *array.FixedSizeList:
		return nil

	default:
		return nil
	}
}

// handleNestedStruct handles filling a nested struct (dynamic sub-message) from an Arrow struct array.
func handleNestedStruct(parentMsg *dynamicpb.Message, fieldDesc protoreflect.FieldDescriptor, structArr *array.Struct, rowIndex int) error {
	if structArr.IsNull(rowIndex) {
		// entire struct is null
		return nil
	}

	// Create a new dynamic sub-message
	nestedMsg := dynamicpb.NewMessage(fieldDesc.Message())
	for i := 0; i < structArr.NumField(); i++ {
		subFieldDesc := fieldDesc.Message().Fields().Get(i)
		col := structArr.Field(i)
		val := ExtractArrowValue(col, rowIndex)
		setDynamicField(nestedMsg, subFieldDesc, val)
	}

	// Set the sub-message
	parentMsg.Set(fieldDesc, protoreflect.ValueOfMessage(nestedMsg))
	return nil
}

// RowToDynamicProto builds a single Protobuf message from one row in an Arrow Record.
func RowToDynamicProto(record arrow.Record, msgDesc protoreflect.MessageDescriptor, rowIndex int) (*dynamicpb.Message, error) {
	dynMsg := dynamicpb.NewMessage(msgDesc)
	numFields := int(record.NumCols())

	for colIdx := 0; colIdx < numFields; colIdx++ {
		fieldDesc := msgDesc.Fields().Get(colIdx)
		col := record.Column(colIdx)
		switch arr := col.(type) {
		case *array.Struct:
			if err := handleNestedStruct(dynMsg, fieldDesc, arr, rowIndex); err != nil {
				return nil, err
			}
		default:
			val := ExtractArrowValue(col, rowIndex)
			setDynamicField(dynMsg, fieldDesc, val)
		}
	}

	return dynMsg, nil
}

// RecordToDynamicProtos converts a single Arrow Record (batch) to one Protobuf message per row.
func RecordToDynamicProtos(record arrow.Record, msgDesc protoreflect.MessageDescriptor) ([][]byte, error) {
	rowCount := int(record.NumRows())
	out := make([][]byte, 0, rowCount)

	for row := 0; row < rowCount; row++ {
		dynMsg, err := RowToDynamicProto(record, msgDesc, row)
		if err != nil {
			return nil, err
		}
		bytes, err := proto.Marshal(dynMsg)
		if err != nil {
			return nil, err
		}
		out = append(out, bytes)
	}
	return out, nil
}

// ArrowReaderToProtos reads from an array.RecordReader and produces a slice of Protobuf messages
// in row-based format. For extremely large data, you may want to stream rather than build a giant slice.
func ArrowReaderToProtos(reader array.RecordReader, msgDesc protoreflect.MessageDescriptor) ([][]byte, error) {
	defer reader.Release()

	var results [][]byte
	for reader.Next() {
		rec := reader.Record()
		rowMsgs, err := RecordToDynamicProtos(rec, msgDesc)
		if err != nil {
			return nil, err
		}
		results = append(results, rowMsgs...)
	}

	return results, reader.Err()
}

// ConvertInParallel is an optional helper that divides the rows of a single Arrow Record
// among multiple goroutines, each producing row-based Protobuf messages. This can
// significantly improve throughput on large arrays if the overhead of marshalling is high.
func ConvertInParallel(ctx context.Context, record arrow.Record, msgDesc protoreflect.MessageDescriptor, concurrency int) ([][]byte, error) {
	rowCount := int(record.NumRows())
	if concurrency < 1 {
		concurrency = 1
	}
	// Divide the rows among workers
	chunkSize := (rowCount + concurrency - 1) / concurrency

	// We'll collect results in a big slice
	results := make([][]byte, rowCount)
	var wg sync.WaitGroup
	var mu sync.Mutex // to guard write into 'results'
	var firstErr error

	for w := 0; w < concurrency; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > rowCount {
			end = rowCount
		}
		if start >= end {
			break
		}
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			localBuf := make([][]byte, 0, end-start)
			for row := start; row < end; row++ {
				select {
				case <-ctx.Done():
					// Cancel
					return
				default:
				}
				dynMsg, err := RowToDynamicProto(record, msgDesc, row)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				b, err := proto.Marshal(dynMsg)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				localBuf = append(localBuf, b)
			}
			// Store localBuf into results
			mu.Lock()
			for i, b := range localBuf {
				results[start+i] = b
			}
			mu.Unlock()
		}(start, end)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// CreateArrowRecord creates a sample Arrow RecordBatch with some data.
func CreateArrowRecord() (array.RecordReader, error) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Append some data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie", "Diana"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2, 76.8, 88.0}, nil)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// FormatArrowJSON formats Arrow records as pretty-printed JSON for debugging.
func FormatArrowJSON(reader array.RecordReader, output io.Writer) error {
	defer reader.Release()

	var records []map[string]interface{}

	for reader.Next() {
		record := reader.Record()

		for row := 0; row < int(record.NumRows()); row++ {
			rowData := make(map[string]interface{})
			for colIdx, field := range record.Schema().Fields() {
				col := record.Column(colIdx)
				rowData[field.Name] = ExtractArrowValue(col, row)
			}
			records = append(records, rowData)
		}
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading records: %w", err)
	}

	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to format JSON: %w", err)
	}

	if _, err := output.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write JSON: %w", err)
	}
	return nil
}
