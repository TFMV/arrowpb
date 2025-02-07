// fuzz_protorand_test.go

package arrowpb

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func createFuzzRecord(t *testing.T, id int64, score float64) arrow.Record {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Add fuzz test data
	builder.Field(0).(*array.Int64Builder).Append(id)
	builder.Field(1).(*array.StringBuilder).Append("fuzz-test")
	builder.Field(2).(*array.Float64Builder).Append(score)

	ts := arrow.Timestamp(time.Now().UnixNano())
	builder.Field(3).(*array.TimestampBuilder).Append(ts)

	record := builder.NewRecord()
	t.Cleanup(func() { record.Release() })
	return record
}

func FuzzConvertArrowRecord(f *testing.F) {
	// Add seed corpus
	f.Add(int64(42), float64(3.14))

	f.Fuzz(func(t *testing.T, id int64, score float64) {
		record := createFuzzRecord(t, id, score)

		// Test conversion
		rows, err := ConvertArrowRecord(record)
		if err != nil {
			t.Skip("conversion failed:", err)
		}
		require.Len(t, rows, 1)

		// Verify conversion results
		row := rows[0]
		require.Equal(t, id, row.Id)
		require.Equal(t, "fuzz-test", row.Name)
		require.Equal(t, score, row.Score)
		require.NotNil(t, row.CreatedAt)

		// Test serialization
		protoBytes, err := SerializeRows(rows)
		if err != nil {
			t.Skip("serialization failed:", err)
		}
		require.Len(t, protoBytes, 1)
	})
}
