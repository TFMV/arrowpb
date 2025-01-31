package arrowpb

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

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

func TestArrowSchemaToProto(t *testing.T) {
	t.Parallel()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	descriptor := arrowSchemaToProto(schema)

	// Verify descriptor fields
	assert.True(t, strings.HasPrefix(*descriptor.Name, "ArrowMessage_"))
	assert.Len(t, descriptor.Field, 3)

	// Verify field types
	fieldTypes := make(map[string]string)
	for _, field := range descriptor.Field {
		fieldTypes[*field.Name] = field.Type.String()
	}

	assert.Equal(t, "TYPE_INT64", fieldTypes["id"])
	assert.Equal(t, "TYPE_STRING", fieldTypes["name"])
	assert.Equal(t, "TYPE_BOOL", fieldTypes["active"])
}

func TestCreateArrowRecord(t *testing.T) {
	t.Parallel()
	reader, err := createArrowRecord()
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
	assert.Equal(t, int64(3), record.NumRows())

	// Check column values
	idCol := record.Column(0).(*array.Int64)
	nameCol := record.Column(1).(*array.String)
	scoreCol := record.Column(2).(*array.Float64)

	assert.Equal(t, int64(1), idCol.Value(0))
	assert.Equal(t, "Alice", nameCol.Value(0))
	assert.Equal(t, 95.5, scoreCol.Value(0))
}

func TestArrowBatchToProto(t *testing.T) {
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

	// Add test data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob"}, nil)
	builder.Field(2).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// Convert Arrow batch to Protobuf messages
	protoDescriptor := arrowSchemaToProto(schema)
	protoMessages := arrowBatchToProto(reader, protoDescriptor)

	// Verify number of messages
	assert.Equal(t, 2, len(protoMessages))

	// Decode each Protobuf message and verify its contents
	for i, protoMsg := range protoMessages {
		decodedProto := &descriptorpb.DescriptorProto{}
		err := proto.Unmarshal(protoMsg, decodedProto)
		require.NoError(t, err, "Failed to unmarshal Protobuf message")

		// Extract values from decoded message
		fields := decodedProto.GetField()
		assert.Len(t, fields, 3) // We expect 3 fields: id, name, active

		expectedValues := map[string]string{
			"id":     "1",
			"name":   "Alice",
			"active": "true",
		}
		if i == 1 {
			expectedValues = map[string]string{
				"id":     "2",
				"name":   "Bob",
				"active": "false",
			}
		}

		for _, field := range fields {
			fieldName := field.GetName()
			assert.Contains(t, expectedValues, fieldName)
		}
	}
}

func TestArrowNestedFields(t *testing.T) {
	t.Parallel()
	// Create test data with nested fields
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

	// Add test data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	userBuilder := builder.Field(1).(*array.StructBuilder)
	userBuilder.Append(true)
	userBuilder.FieldBuilder(0).(*array.StringBuilder).Append("Alice")
	userBuilder.FieldBuilder(1).(*array.Int64Builder).Append(30)

	record := builder.NewRecord()
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)

	// Verify nested field values
	assert.True(t, reader.Next())
	nestedRecord := reader.Record().Column(1).(*array.Struct)
	assert.Equal(t, "Alice", nestedRecord.Field(0).(*array.String).Value(0))
	assert.Equal(t, int64(30), nestedRecord.Field(1).(*array.Int64).Value(0))
}
