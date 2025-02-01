package arrowpb

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestFormatArrowJSON verifies that Arrow records are pretty-printed as JSON.
func TestFormatArrowJSON(t *testing.T) {
	t.Parallel()

	// Create test data
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Add test data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2}, nil)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)

	// Test JSON formatting
	var buf bytes.Buffer
	err = FormatArrowJSON(reader, &buf)
	require.NoError(t, err)

	// Verify JSON structure
	var result []map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	// Verify content
	expected := []map[string]interface{}{
		{"id": float64(1), "name": "Alice", "score": 95.5},
		{"id": float64(2), "name": "Bob", "score": 89.2},
	}
	assert.Equal(t, expected, result)
}

// TestArrowSchemaToFileDescriptorProto tests generating a descriptor from a simple Arrow schema.
func TestArrowSchemaToFileDescriptorProto(t *testing.T) {
	t.Parallel()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.arrowpb", "ArrowMessage", nil)
	require.NoError(t, err, "failed to build file descriptor proto")

	// Check top-level message name
	assert.True(t, strings.HasPrefix(fdp.GetMessageType()[0].GetName(), "ArrowMessage_"))
	assert.Len(t, fdp.GetMessageType()[0].GetField(), 3)

	// Compile descriptor
	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err, "failed to compile file descriptor proto")

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err, "failed to get top-level message descriptor")

	// Verify the fields match the schema
	require.Equal(t, 3, msgDesc.Fields().Len())

	// Check field types via the final compiled descriptor
	checkField := func(idx int, name string, kind protoreflect.Kind) {
		f := msgDesc.Fields().Get(idx)
		assert.Equal(t, name, string(f.Name()))
		assert.Equal(t, kind, f.Kind())
	}

	checkField(0, "id", protoreflect.Int64Kind)
	checkField(1, "name", protoreflect.StringKind)
	checkField(2, "active", protoreflect.BoolKind)

	// (Optional) print the descriptor for debugging
	descriptorText, _ := prototext.Marshal(fdp)
	t.Logf("Compiled Descriptor:\n%s", descriptorText)
}

// TestCreateArrowRecord ensures our sample record creator works as expected.
func TestCreateArrowRecord(t *testing.T) {
	t.Parallel()

	reader, err := CreateArrowRecord()
	require.NoError(t, err)
	defer reader.Release()

	// Verify schema
	schema := reader.Schema()
	assert.Equal(t, 3, len(schema.Fields()))
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, "score", schema.Field(2).Name)

	// Verify data
	assert.True(t, reader.Next())
	record := reader.Record()
	require.EqualValues(t, 4, record.NumRows())

	// Check column values
	idCol := record.Column(0).(*array.Int64)
	nameCol := record.Column(1).(*array.String)
	scoreCol := record.Column(2).(*array.Float64)

	assert.Equal(t, int64(1), idCol.Value(0))
	assert.Equal(t, "Alice", nameCol.Value(0))
	assert.Equal(t, 95.5, scoreCol.Value(0))
}

// TestArrowReaderToProtos ensures that Arrow data is converted into dynamic Protobuf messages properly.
func TestArrowReaderToProtos(t *testing.T) {
	t.Parallel()

	// Create test data
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob"}, nil)
	builder.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// 1. Build a FileDescriptorProto
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.arrowpb", "ArrowMessage", nil)
	require.NoError(t, err)

	// 2. Compile it
	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)

	// 3. Get top-level message descriptor
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	// 4. Convert Arrow data -> Proto messages (wire format)
	protoMessages, err := ArrowReaderToProtos(context.Background(), reader, msgDesc, nil)
	require.NoError(t, err)

	// We should have 2 messages total
	require.Len(t, protoMessages, 2)

	// 5. Unmarshal each message and verify
	for i, msgBytes := range protoMessages {
		dynMsg := dynamicpb.NewMessage(msgDesc)
		err := proto.Unmarshal(msgBytes, dynMsg)
		require.NoError(t, err, "Failed to unmarshal dynamic message")

		// Check fields
		idVal := dynMsg.Get(msgDesc.Fields().ByName("id"))
		nameVal := dynMsg.Get(msgDesc.Fields().ByName("name"))
		activeVal := dynMsg.Get(msgDesc.Fields().ByName("active"))

		if i == 0 {
			assert.EqualValues(t, 1, idVal.Int())
			assert.Equal(t, "Alice", nameVal.String())
			assert.Equal(t, true, activeVal.Bool())
		} else {
			assert.EqualValues(t, 2, idVal.Int())
			assert.Equal(t, "Bob", nameVal.String())
			assert.Equal(t, false, activeVal.Bool())
		}
	}
}

// TestArrowNestedFields checks that we can handle a struct field with nested columns.
func TestArrowNestedFields(t *testing.T) {
	t.Parallel()

	// Create a schema with a nested (Struct) field
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "user", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Populate row #1
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	userBuilder := builder.Field(1).(*array.StructBuilder)
	userBuilder.Append(true)
	userBuilder.FieldBuilder(0).(*array.StringBuilder).Append("Alice")
	userBuilder.FieldBuilder(1).(*array.Int64Builder).Append(30)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// 1. Convert the Arrow schema -> descriptor
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.nested", "NestedTest", nil)
	require.NoError(t, err, "failed to build FileDescriptorProto")

	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err, "failed to compile file descriptor")

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err, "failed to get message descriptor")

	// 2. Convert entire record to Protos
	protoMsgs, err := ArrowReaderToProtos(context.Background(), reader, msgDesc, nil)
	require.NoError(t, err)
	require.Len(t, protoMsgs, 1, "should have exactly one row")

	// 3. Decode dynamic message
	dynMsg := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protoMsgs[0], dynMsg))

	// Check top-level "id"
	idVal := dynMsg.Get(msgDesc.Fields().ByName("id"))
	assert.EqualValues(t, 1, idVal.Int())

	// user is a MessageKind field
	userField := msgDesc.Fields().ByName("user")
	userMsg := dynMsg.Get(userField).Message()

	// Check user.name, user.age
	nameVal := userMsg.Get(userField.Message().Fields().ByName("name"))
	ageVal := userMsg.Get(userField.Message().Fields().ByName("age"))

	assert.Equal(t, "Alice", nameVal.String())
	assert.EqualValues(t, 30, ageVal.Int())
}

// ----------------------------------------------------------------------------
//  New “Beefed Up” Tests
// ----------------------------------------------------------------------------

// TestDescriptorCache checks that calling ArrowSchemaToFileDescriptorProto multiple times
// with the same schema reuses a cached descriptor if ConvertConfig.DescriptorCache is set.
func TestDescriptorCache(t *testing.T) {
	t.Parallel()

	cfg := &ConvertConfig{}
	// Initialize the cache
	cfg.DescriptorCache = *new(sync.Map)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Call once
	fdp1, err := ArrowSchemaToFileDescriptorProto(schema, "cache.test", "Cached", cfg)
	require.NoError(t, err)

	// Call again, same schema
	fdp2, err := ArrowSchemaToFileDescriptorProto(schema, "cache.test", "Cached", cfg)
	require.NoError(t, err)

	// They should be the exact same pointer from the sync.Map (or at least logically identical).
	// We'll check pointers for equality. If not guaranteed, we can do a deeper check:
	assert.Equal(t, fdp1, fdp2, "expected to use the cached descriptor")
}

// TestProto2Syntax ensures that UseProto2Syntax = true sets the `.proto` to "proto2".
func TestProto2Syntax(t *testing.T) {
	t.Parallel()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cfg := &ConvertConfig{
		UseProto2Syntax: true,
	}
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "proto2.test", "P2", cfg)
	require.NoError(t, err)
	assert.Equal(t, "proto2", fdp.GetSyntax())
}

// TestWrapperTypes verifies that arrow fields become well-known wrapper messages.
func TestWrapperTypes(t *testing.T) {
	t.Parallel()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int64}, // -> google.protobuf.Int64Value
		{Name: "ok", Type: arrow.FixedWidthTypes.Boolean}, // -> google.protobuf.BoolValue
	}, nil)

	cfg := &ConvertConfig{
		UseWrapperTypes: true,
	}
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "wrapper.test", "WrapMsg", cfg)
	require.NoError(t, err)

	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)
	require.Equal(t, 2, msgDesc.Fields().Len())

	// Check that the descriptor references wrapper types
	countField := msgDesc.Fields().ByName("count")
	assert.Equal(t, protoreflect.MessageKind, countField.Kind())
	assert.Equal(t, "google.protobuf.Int64Value", string(countField.Message().FullName()))

	okField := msgDesc.Fields().ByName("ok")
	assert.Equal(t, protoreflect.MessageKind, okField.Kind())
	assert.Equal(t, "google.protobuf.BoolValue", string(okField.Message().FullName()))
}

// TestWellKnownTimestamps verifies arrow.TIMESTAMP => google.protobuf.Timestamp if UseWellKnownTimestamps is set.
func TestWellKnownTimestamps(t *testing.T) {
	t.Parallel()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us},
	}, nil)

	cfg := &ConvertConfig{
		UseWellKnownTimestamps: true,
	}
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "timestamp.test", "TSMsg", cfg)
	require.NoError(t, err)

	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)
	require.Equal(t, 1, msgDesc.Fields().Len())

	// Check that the descriptor references google.protobuf.Timestamp
	tsField := msgDesc.Fields().ByName("ts")
	assert.Equal(t, protoreflect.MessageKind, tsField.Kind())
	assert.Equal(t, "google.protobuf.Timestamp", string(tsField.Message().FullName()))

	// Verify actual data conversion: build a record with 2 rows
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	tsb := builder.Field(0).(*array.TimestampBuilder)
	now := time.Now()
	tsb.Append(arrow.Timestamp(now.UnixMicro()))
	tsb.Append(arrow.Timestamp(now.Add(time.Second * 10).UnixMicro()))

	rec := builder.NewRecord()
	defer rec.Release()

	rows, err := RecordToDynamicProtos(rec, msgDesc, cfg)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

// TestDictionaryAsEnum ensures that we can mark dictionary columns as enums if MapDictionariesToEnums is true.
func TestDictionaryAsEnum(t *testing.T) {
	t.Parallel()

	// Build a simple dictionary-encoded column
	// For demonstration, let's create a dictionary with "apple","banana","cherry".
	// We'll assign row 0 => "apple", row 1 => "banana".
	mem := memory.NewGoAllocator()
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: arrow.BinaryTypes.String,
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "fruit", Type: dictType},
	}, nil)

	// Build a dictionary array
	// In arrow v18 or so, you might do something like array.NewDictionaryBuilder
	// We'll do a minimal approach.
	indexBuilder := array.NewInt8Builder(mem)
	defer indexBuilder.Release()
	indexBuilder.AppendValues([]int8{0, 1}, nil)

	// The dictionary itself (the distinct values)
	dictValuesBuilder := array.NewStringBuilder(mem)
	dictValuesBuilder.AppendValues([]string{"apple", "banana", "cherry"}, nil)
	dictValues := dictValuesBuilder.NewArray()
	dictValuesBuilder.Release()

	// Construct DictionaryArray
	dictArr := array.NewDictionaryArray(dictType, indexBuilder.NewArray(), dictValues)
	defer dictArr.Release()

	record := array.NewRecord(schema, []arrow.Array{dictArr}, 2)
	defer record.Release()

	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// Create a config with MapDictionariesToEnums = true
	cfg := &ConvertConfig{
		MapDictionariesToEnums: true,
	}

	// Convert schema -> descriptor
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "dictenum.test", "DictEnum", cfg)
	require.NoError(t, err)

	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	fruitField := msgDesc.Fields().ByName("fruit")
	// We expect the field to be TYPE_ENUM
	require.Equal(t, protoreflect.EnumKind, fruitField.Kind(),
		"dictionary-encoded column should be mapped to an enum")

	// Convert data
	protos, err := ArrowReaderToProtos(context.Background(), reader, msgDesc, cfg)
	require.NoError(t, err)
	require.Len(t, protos, 2)

	// For row 0 => index 0 => we expect the enum number = 0. row1 => 1 => ...
	// We can check the dynamic message's enum.
	dyn1 := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protos[0], dyn1))
	enumVal1 := dyn1.Get(fruitField).Enum()
	assert.EqualValues(t, 0, enumVal1) // "apple"

	dyn2 := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protos[1], dyn2))
	enumVal2 := dyn2.Get(fruitField).Enum()
	assert.EqualValues(t, 1, enumVal2) // "banana"
}

// TestNestedList ensures that a repeated list column is handled as repeated fields in proto.
func TestNestedList(t *testing.T) {
	t.Parallel()

	// Example: A column of list<int64>
	mem := memory.NewGoAllocator()
	field := arrow.Field{Name: "nums", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)

	// Build a single Record with two rows:
	// Row 0 => [1,2,3]
	// Row 1 => [10,20]
	listBuilder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer listBuilder.Release()

	valBuilder := listBuilder.ValueBuilder().(*array.Int64Builder)

	// Row 0
	listBuilder.Append(true)
	valBuilder.AppendValues([]int64{1, 2, 3}, nil)

	// Row 1
	listBuilder.Append(true)
	valBuilder.AppendValues([]int64{10, 20}, nil)

	arr := listBuilder.NewArray()
	defer arr.Release()

	record := array.NewRecord(schema, []arrow.Array{arr}, 2)
	defer record.Release()

	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// Create descriptor
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "lists.test", "ListMsg", nil)
	require.NoError(t, err)
	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	// The single field "nums" should be repeated int64
	numsField := msgDesc.Fields().ByName("nums")
	require.Equal(t, true, numsField.IsList())
	require.Equal(t, protoreflect.Int64Kind, numsField.Kind())

	// Convert rows
	protos, err := ArrowReaderToProtos(context.Background(), reader, msgDesc, nil)
	require.NoError(t, err)
	require.Len(t, protos, 2)

	// Decode each row
	dyn0 := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protos[0], dyn0))
	// "nums" repeated => [1,2,3]
	listVal := dyn0.Get(numsField).List()
	require.EqualValues(t, 3, listVal.Len())
	assert.EqualValues(t, 1, listVal.Get(0).Int())
	assert.EqualValues(t, 2, listVal.Get(1).Int())
	assert.EqualValues(t, 3, listVal.Get(2).Int())

	dyn1 := dynamicpb.NewMessage(msgDesc)
	require.NoError(t, proto.Unmarshal(protos[1], dyn1))
	listVal2 := dyn1.Get(numsField).List()
	require.EqualValues(t, 2, listVal2.Len())
	assert.EqualValues(t, 10, listVal2.Get(0).Int())
	assert.EqualValues(t, 20, listVal2.Get(1).Int())
}

// TestConvertInParallel ensures concurrency-based conversion is correct.
func TestConvertInParallel(t *testing.T) {
	t.Parallel()

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "index", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create a large record of 10 rows
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	testVals := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	builder.Field(0).(*array.Int64Builder).AppendValues(testVals, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Build descriptor
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "parallel.test", "ParMsg", nil)
	require.NoError(t, err)
	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	// Convert in parallel
	out, err := ConvertInParallel(context.Background(), record, msgDesc, 3, nil)
	require.NoError(t, err)
	require.Len(t, out, len(testVals))

	// Verify each row
	for i, msgBytes := range out {
		dynMsg := dynamicpb.NewMessage(msgDesc)
		require.NoError(t, proto.Unmarshal(msgBytes, dynMsg))
		idxVal := dynMsg.Get(msgDesc.Fields().ByName("index"))
		assert.EqualValues(t, testVals[i], idxVal.Int())
	}
}
