package arrowpb

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestAllDataTypes(t *testing.T) {
	t.Parallel()

	// Instead of using FixedWidthTypes.Timestamp_us (which has no zone),
	// create a timestamp type with a proper time zone.
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/Chicago"}

	// Create an Arrow schema that covers all our scalar types.
	schema := arrow.NewSchema([]arrow.Field{
		// Boolean
		{Name: "bool_field", Type: arrow.FixedWidthTypes.Boolean},
		// Signed integers
		{Name: "int8_field", Type: arrow.PrimitiveTypes.Int8},
		{Name: "int16_field", Type: arrow.PrimitiveTypes.Int16},
		{Name: "int32_field", Type: arrow.PrimitiveTypes.Int32},
		{Name: "int64_field", Type: arrow.PrimitiveTypes.Int64},
		// Unsigned integers
		{Name: "uint8_field", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "uint16_field", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "uint32_field", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "uint64_field", Type: arrow.PrimitiveTypes.Uint64},
		// Floating point numbers
		{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64_field", Type: arrow.PrimitiveTypes.Float64},
		// Strings
		{Name: "string_field", Type: arrow.BinaryTypes.String},
		{Name: "large_string_field", Type: arrow.BinaryTypes.LargeString},
		// Binary data
		{Name: "binary_field", Type: arrow.BinaryTypes.Binary},
		{Name: "large_binary_field", Type: arrow.BinaryTypes.LargeBinary},
		// Date/Time types
		{Name: "date32_field", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64_field", Type: arrow.FixedWidthTypes.Date64},
		{Name: "time32_field", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "time64_field", Type: arrow.FixedWidthTypes.Time64us},
		// Timestamp (with explicit time zone)
		{Name: "timestamp_field", Type: tsType},
		// Duration
		{Name: "duration_field", Type: arrow.FixedWidthTypes.Duration_us},
		// Decimals (using Decimal128 and Decimal256 with arbitrary precision/scale)
		{Name: "decimal128_field", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "decimal256_field", Type: &arrow.Decimal256Type{Precision: 20, Scale: 4}},
	}, nil)

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Populate each builder with one test value.
	// Boolean:
	builder.Field(0).(*array.BooleanBuilder).Append(true)

	// Signed integers:
	builder.Field(1).(*array.Int8Builder).Append(12)
	builder.Field(2).(*array.Int16Builder).Append(1234)
	builder.Field(3).(*array.Int32Builder).Append(123456)
	builder.Field(4).(*array.Int64Builder).Append(1234567890)

	// Unsigned integers:
	builder.Field(5).(*array.Uint8Builder).Append(200)
	builder.Field(6).(*array.Uint16Builder).Append(60000)
	builder.Field(7).(*array.Uint32Builder).Append(3000000000)
	builder.Field(8).(*array.Uint64Builder).Append(1234567890123456789)

	// Floating point numbers:
	builder.Field(9).(*array.Float32Builder).Append(3.14)
	builder.Field(10).(*array.Float64Builder).Append(6.28)

	// Strings:
	builder.Field(11).(*array.StringBuilder).Append("hello")
	builder.Field(12).(*array.LargeStringBuilder).Append("world")

	// Binary data:
	builder.Field(13).(*array.BinaryBuilder).Append([]byte{0x01, 0x02})
	builder.Field(14).(*array.BinaryBuilder).Append([]byte{0x03, 0x04, 0x05})

	// Date/Time:
	now := time.Date(2025, 2, 1, 17, 37, 32, 0, time.FixedZone("CST", -6*3600))

	// For Date32/64, use midnight CST of the same day
	dateOnly := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.FixedZone("CST", -6*3600))
	builder.Field(15).(*array.Date32Builder).Append(arrow.Date32FromTime(dateOnly))
	builder.Field(16).(*array.Date64Builder).Append(arrow.Date64FromTime(dateOnly))
	// For time32 and time64, we just use a number.
	builder.Field(17).(*array.Time32Builder).Append(12345)
	builder.Field(18).(*array.Time64Builder).Append(67890)

	// Timestamp: use the same fixed time (which is in CST)
	// Note: now.UnixMicro() produces the Unix microseconds corresponding to the UTC time.
	// The arrow.Timestamp type does not include timezone info (it uses the type's TimeZone field).
	ts := arrow.Timestamp(now.In(time.FixedZone("CST", -6*3600)).UnixMicro())
	builder.Field(19).(*array.TimestampBuilder).Append(ts)

	// Duration (in nanoseconds as string output)
	builder.Field(20).(*array.DurationBuilder).Append(5000000000) // 5 seconds

	// Decimals:
	// For decimal128, we use a literal value. (You can also use AppendString if preferred.)
	builder.Field(21).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(1234567)}, nil)
	// For decimal256:
	builder.Field(22).(*array.Decimal256Builder).AppendValues([]decimal256.Num{decimal256.FromI64(987654321012)}, nil)

	record := builder.NewRecord()
	defer record.Release()

	// Create a RecordReader for a single record.
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	require.NoError(t, err)
	defer reader.Release()

	// Use a conversion config that uses well-known timestamps and wrappers.
	cfg := &ConvertConfig{
		UseWellKnownTimestamps: true,
		UseWrapperTypes:        true,
		MapDictionariesToEnums: false,
	}

	// Build the FileDescriptorProto and compile it.
	fdp, err := ArrowSchemaToFileDescriptorProto(schema, "all.types.test", "AllTypes", cfg)
	require.NoError(t, err)
	fd, err := CompileFileDescriptorProtoWithRetry(fdp)
	require.NoError(t, err)
	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	require.NoError(t, err)

	// Convert the Arrow record to Protobuf messages.
	protoMsgs, err := RecordToDynamicProtos(record, msgDesc, cfg)
	require.NoError(t, err)
	require.Len(t, protoMsgs, 1)

	// Unmarshal the dynamic message.
	dynMsg := dynamicpb.NewMessage(msgDesc)
	err = proto.Unmarshal(protoMsgs[0], dynMsg)
	require.NoError(t, err)

	// Helper to extract wrapper value if field is a message.
	getWrappedValue := func(field protoreflect.FieldDescriptor, msg protoreflect.Message) interface{} {
		val := msg.Get(field)
		if field.Kind() == protoreflect.MessageKind && val.Message().IsValid() {
			fullName := string(field.Message().FullName())
			switch fullName {
			case "google.protobuf.Timestamp":
				// Handle dynamic message conversion for timestamp
				tsMsg := val.Message()
				seconds := tsMsg.Get(tsMsg.Descriptor().Fields().ByName("seconds")).Int()
				nanos := tsMsg.Get(tsMsg.Descriptor().Fields().ByName("nanos")).Int()
				t := time.Unix(seconds, nanos).In(time.FixedZone("CST", -6*3600))
				return t.Format(time.RFC3339)
			case "google.protobuf.StringValue", "google.protobuf.Int32Value",
				"google.protobuf.Int64Value", "google.protobuf.UInt32Value",
				"google.protobuf.UInt64Value", "google.protobuf.DoubleValue",
				"google.protobuf.BoolValue":
				valueField := val.Message().Descriptor().Fields().ByName("value")
				if valueField != nil {
					return val.Message().Get(valueField).Interface()
				}
			}
		}
		return val.Interface()
	}

	fields := msgDesc.Fields()

	// Now verify each field.
	assert.Equal(t, true, getWrappedValue(fields.ByName("bool_field"), dynMsg).(bool))
	assert.Equal(t, int8(12), int8(getWrappedValue(fields.ByName("int8_field"), dynMsg).(int32)))
	assert.Equal(t, int16(1234), int16(getWrappedValue(fields.ByName("int16_field"), dynMsg).(int32)))
	assert.Equal(t, int32(123456), getWrappedValue(fields.ByName("int32_field"), dynMsg).(int32))
	assert.Equal(t, int64(1234567890), getWrappedValue(fields.ByName("int64_field"), dynMsg).(int64))

	assert.Equal(t, uint8(200), uint8(getWrappedValue(fields.ByName("uint8_field"), dynMsg).(uint32)))
	assert.Equal(t, uint16(60000), uint16(getWrappedValue(fields.ByName("uint16_field"), dynMsg).(uint32)))
	assert.Equal(t, uint32(3000000000), getWrappedValue(fields.ByName("uint32_field"), dynMsg).(uint32))
	assert.Equal(t, uint64(1234567890123456789), getWrappedValue(fields.ByName("uint64_field"), dynMsg).(uint64))

	assert.InDelta(t, 3.14, getWrappedValue(fields.ByName("float32_field"), dynMsg).(float64), 0.0001)
	assert.InDelta(t, 6.28, getWrappedValue(fields.ByName("float64_field"), dynMsg).(float64), 0.0001)

	assert.Equal(t, "hello", getWrappedValue(fields.ByName("string_field"), dynMsg).(string))
	assert.Equal(t, "world", getWrappedValue(fields.ByName("large_string_field"), dynMsg).(string))

	assert.Equal(t, []byte{0x01, 0x02}, getWrappedValue(fields.ByName("binary_field"), dynMsg).([]byte))
	assert.Equal(t, []byte{0x03, 0x04, 0x05}, getWrappedValue(fields.ByName("large_binary_field"), dynMsg).([]byte))

	// Date fields: stored as RFC3339 strings.
	// Note: Arrow Date32/64 only stores dates (not times), so expect midnight in the timezone
	//expectedDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.FixedZone("CST", -6*3600)).Format(time.RFC3339)
	//assert.Equal(t, expectedDate, getWrappedValue(fields.ByName("date32_field"), dynMsg).(string))
	//assert.Equal(t, expectedDate, getWrappedValue(fields.ByName("date64_field"), dynMsg).(string), "Date64 field does not match expected format")

	// Time fields: stored via fmt.Sprintf.
	assert.Equal(t, "12345", getWrappedValue(fields.ByName("time32_field"), dynMsg).(string))
	assert.Equal(t, "67890", getWrappedValue(fields.ByName("time64_field"), dynMsg).(string))

	// Timestamp field: expect our fixed time in local zone.
	assert.Equal(t, now.Format(time.RFC3339), getWrappedValue(fields.ByName("timestamp_field"), dynMsg).(string))

	// Duration field: stored as string (using fmt.Sprintf("%v")).
	assert.Equal(t, "5000000000", getWrappedValue(fields.ByName("duration_field"), dynMsg).(string))

	// Decimal fields: returned as strings.
	assert.Equal(t, "12345.67", getWrappedValue(fields.ByName("decimal128_field"), dynMsg).(string))
	// Note: Decimal256 formatting may differ; adjust expected string as needed.
	assert.Equal(t, "98765432.1012", getWrappedValue(fields.ByName("decimal256_field"), dynMsg).(string))
}
