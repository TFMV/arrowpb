package arrowpb

import (
	"testing"
	"time"

	arrowpb "github.com/TFMV/arrowpb/proto"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func createTestRecord(t *testing.T) arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Add test data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2, 76.8}, nil)

	// Create timestamps
	now := time.Now()
	ts := arrow.Timestamp(now.UnixNano())
	builder.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{ts, ts, ts}, nil)

	record := builder.NewRecord()
	t.Cleanup(func() { record.Release() })
	return record
}

func TestConvertArrowRecord(t *testing.T) {
	record := createTestRecord(t)

	// Test conversion
	rows, err := ConvertArrowRecord(record)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	// Verify first row
	assert.Equal(t, int64(1), rows[0].Id)
	assert.Equal(t, "Alice", rows[0].Name)
	assert.Equal(t, 95.5, rows[0].Score)
	assert.NotNil(t, rows[0].CreatedAt)

	// Test serialization
	protoBytes, err := SerializeRows(rows)
	require.NoError(t, err)
	require.Len(t, protoBytes, 3)

	// Test deserialization of first row
	var deserializedRow arrowpb.Row
	err = proto.Unmarshal(protoBytes[0], &deserializedRow)
	require.NoError(t, err)
	assert.Equal(t, rows[0].Id, deserializedRow.Id)
	assert.Equal(t, rows[0].Name, deserializedRow.Name)
	assert.Equal(t, rows[0].Score, deserializedRow.Score)
	assert.Equal(t, rows[0].CreatedAt.Seconds, deserializedRow.CreatedAt.Seconds)
}

func TestConvertArrowRecordWithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Add test data with nulls
	builder.Field(0).(*array.Int64Builder).AppendNull()
	builder.Field(1).(*array.StringBuilder).AppendNull()
	builder.Field(2).(*array.Float64Builder).AppendNull()
	builder.Field(3).(*array.TimestampBuilder).AppendNull()

	record := builder.NewRecord()
	defer record.Release()

	rows, err := ConvertArrowRecord(record)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Verify null handling
	assert.Equal(t, int64(0), rows[0].Id)
	assert.Equal(t, "", rows[0].Name)
	assert.Equal(t, float64(0), rows[0].Score)
	assert.Nil(t, rows[0].CreatedAt)
}
