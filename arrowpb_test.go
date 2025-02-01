package arrowpb

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

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

	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.arrowpb", "ArrowMessage")
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
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.arrowpb", "ArrowMessage")
	require.NoError(t, err)

	// 2. Compile it
	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err)

	// 3. Get top-level message descriptor
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	// 4. Convert Arrow data -> Proto messages (wire format)
	protoMessages, err := ArrowReaderToProtos(context.Background(), reader, msgDesc)
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
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "example.nested", "NestedTest")
	require.NoError(t, err, "failed to build FileDescriptorProto")

	fd, err := CompileFileDescriptorProto(fdp)
	require.NoError(t, err, "failed to compile file descriptor")

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err, "failed to get message descriptor")

	// 2. Convert entire record to Protos
	protoMsgs, err := ArrowReaderToProtos(context.Background(), reader, msgDesc)
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
