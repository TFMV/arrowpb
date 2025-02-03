package arrowpb

import (
	"fmt"
	"strings"
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
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func init() {
	// Register well-known type descriptors
	files := []protoreflect.FileDescriptor{
		wrapperspb.File_google_protobuf_wrappers_proto,
		timestamppb.File_google_protobuf_timestamp_proto,
	}

	for _, fd := range files {
		name := fd.Path()
		if _, err := protoregistry.GlobalFiles.FindFileByPath(name); err == protoregistry.NotFound {
			if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
				panic(fmt.Sprintf("failed to register well-known type %s: %v", name, err))
			}
		}
	}
}

// unwrapWrapper is a helper to extract the underlying value from a wrapper message.
func unwrapWrapper(fd protoreflect.FieldDescriptor, val protoreflect.Value) interface{} {
	// If the field is a message and its full name begins with "google.protobuf."
	// then extract the "value" subfield.
	if fd.Kind() == protoreflect.MessageKind && val.Message().IsValid() {
		fullName := string(fd.Message().FullName())
		if strings.HasPrefix(fullName, "google.protobuf.") {
			if vField := val.Message().Descriptor().Fields().ByName("value"); vField != nil {
				return val.Message().Get(vField).Interface()
			}
		}
	}
	return val.Interface()
}

// getDynamicFieldValue is a helper that, given a field descriptor and a dynamic message,
// returns the underlying scalar value (unwrapping wrappers if needed).
func getDynamicFieldValue(fd protoreflect.FieldDescriptor, msg protoreflect.Message) interface{} {
	rawVal := msg.Get(fd)
	if fd.Kind() == protoreflect.MessageKind && rawVal.Message().IsValid() {
		// Unwrap wrapper messages.
		if strings.HasPrefix(string(fd.Message().FullName()), "google.protobuf.") {
			return unwrapWrapper(fd, rawVal)
		}
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return rawVal.Bool()
	case protoreflect.Int32Kind:
		return int32(rawVal.Int())
	case protoreflect.Int64Kind:
		return rawVal.Int()
	case protoreflect.Uint32Kind:
		return uint32(rawVal.Uint())
	case protoreflect.Uint64Kind:
		return rawVal.Uint()
	case protoreflect.FloatKind:
		return float32(rawVal.Float())
	case protoreflect.DoubleKind:
		return rawVal.Float()
	case protoreflect.StringKind:
		return rawVal.String()
	case protoreflect.BytesKind:
		return rawVal.Bytes()
	}
	return rawVal.Interface()
}

// TestAllDataTypes ensures that arrowpb correctly converts an Arrow record containing
// a variety of scalar types into a dynamic Protobuf message. The test runs two scenarios:
// one using wrapper types and well-known timestamps, and one with primitive types.
func TestAllDataTypes(t *testing.T) {
	// Create a timestamp type with an explicit time zone.
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/Chicago"}

	// Build a schema covering many key Arrow scalar types.
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
		// Floating points
		{Name: "float32_field", Type: arrow.PrimitiveTypes.Float32},
		{Name: "float64_field", Type: arrow.PrimitiveTypes.Float64},
		// Strings
		{Name: "string_field", Type: arrow.BinaryTypes.String},
		{Name: "large_string_field", Type: arrow.BinaryTypes.LargeString},
		// Binary
		{Name: "binary_field", Type: arrow.BinaryTypes.Binary},
		{Name: "large_binary_field", Type: arrow.BinaryTypes.LargeBinary},
		// Date/Time
		{Name: "date32_field", Type: arrow.FixedWidthTypes.Date32},
		{Name: "date64_field", Type: arrow.FixedWidthTypes.Date64},
		{Name: "time32_field", Type: arrow.FixedWidthTypes.Time32ms},
		{Name: "time64_field", Type: arrow.FixedWidthTypes.Time64us},
		// Timestamp
		{Name: "timestamp_field", Type: tsType},
		// Duration
		{Name: "duration_field", Type: arrow.FixedWidthTypes.Duration_us},
		// Decimals
		{Name: "decimal128_field", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "decimal256_field", Type: &arrow.Decimal256Type{Precision: 20, Scale: 4}},
	}, nil)

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	t.Cleanup(builder.Release)

	// Populate the builder with a single row of test data.
	builder.Field(0).(*array.BooleanBuilder).Append(true)
	builder.Field(1).(*array.Int8Builder).Append(12)
	builder.Field(2).(*array.Int16Builder).Append(1234)
	builder.Field(3).(*array.Int32Builder).Append(123456)
	builder.Field(4).(*array.Int64Builder).Append(1234567890)

	builder.Field(5).(*array.Uint8Builder).Append(200)
	builder.Field(6).(*array.Uint16Builder).Append(60000)
	builder.Field(7).(*array.Uint32Builder).Append(3000000000)
	builder.Field(8).(*array.Uint64Builder).Append(1234567890123456789)

	builder.Field(9).(*array.Float32Builder).Append(3.14)
	builder.Field(10).(*array.Float64Builder).Append(6.28)

	builder.Field(11).(*array.StringBuilder).Append("hello")
	builder.Field(12).(*array.LargeStringBuilder).Append("world")

	builder.Field(13).(*array.BinaryBuilder).Append([]byte{0x01, 0x02})
	builder.Field(14).(*array.BinaryBuilder).Append([]byte{0x03, 0x04, 0x05})

	now := time.Date(2025, 2, 1, 17, 37, 32, 0, time.FixedZone("CST", -6*3600))
	dateOnly := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	builder.Field(15).(*array.Date32Builder).Append(arrow.Date32FromTime(dateOnly))
	builder.Field(16).(*array.Date64Builder).Append(arrow.Date64FromTime(dateOnly))
	builder.Field(17).(*array.Time32Builder).Append(12345)
	builder.Field(18).(*array.Time64Builder).Append(67890)

	ts := arrow.Timestamp(now.UnixMicro())
	builder.Field(19).(*array.TimestampBuilder).Append(ts)

	// Duration: 5 seconds in microseconds.
	builder.Field(20).(*array.DurationBuilder).Append(arrow.Duration(5000000000))

	builder.Field(21).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(1234567)}, nil)
	builder.Field(22).(*array.Decimal256Builder).AppendValues([]decimal256.Num{decimal256.FromI64(987654321012)}, nil)

	record := builder.NewRecord()
	t.Cleanup(record.Release)

	// Define two test scenarios.
	scenarios := []struct {
		name                   string
		useWrapperTypes        bool
		useWellKnownTimestamps bool
	}{
		{
			name:                   "Wrappers_And_WellKnown_Timestamps",
			useWrapperTypes:        true,
			useWellKnownTimestamps: true,
		},
		{
			name:                   "Unwrapped_Primitives",
			useWrapperTypes:        false,
			useWellKnownTimestamps: false,
		},
	}

	// Loop over scenarios.
	for _, sc := range scenarios {
		sc := sc // capture range variable
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()

			cfg := &ConvertConfig{
				UseWrapperTypes:        sc.useWrapperTypes,
				UseWellKnownTimestamps: sc.useWellKnownTimestamps,
				ForceFlatSchema:        true,
			}

			// Generate and compile the FileDescriptorProto.
			fdp, err := ArrowSchemaToFileDescriptorProto(schema, "all.types.test", "AllTypes", cfg)
			require.NoError(t, err, "ArrowSchemaToFileDescriptorProto failed")

			fd, err := CompileFileDescriptorProtoWithRetry(fdp)
			require.NoError(t, err, "CompileFileDescriptorProtoWithRetry failed")

			msgDesc, err := GetTopLevelMessageDescriptor(fd)
			require.NoError(t, err, "GetTopLevelMessageDescriptor failed")

			// Convert the Arrow record into dynamic proto messages.
			protoMsgs, err := RecordToDynamicProtos(record, msgDesc, cfg)
			require.NoError(t, err, "RecordToDynamicProtos failed")
			require.Len(t, protoMsgs, 1, "expected one proto message")

			// Unmarshal the first (and only) proto message.
			dynMsg := dynamicpb.NewMessage(msgDesc)
			err = proto.Unmarshal(protoMsgs[0], dynMsg)
			require.NoError(t, err, "unmarshal dynamic message failed")

			// getFieldValue is a helper that looks up a field by name and returns its value.
			getFieldValue := func(fieldName string) interface{} {
				t.Helper()
				fd := msgDesc.Fields().ByName(protoreflect.Name(fieldName))
				require.NotNil(t, fd, "field %s not found", fieldName)
				return getDynamicFieldValue(fd, dynMsg)
			}

			// Define a table of field checks.
			tests := []struct {
				fieldName string
				assertFn  func(t *testing.T, actual interface{})
			}{
				{
					fieldName: "bool_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, true, actual)
					},
				},
				{
					fieldName: "int8_field",
					assertFn: func(t *testing.T, actual interface{}) {
						// Underlying type is int32 even for small integers.
						require.IsType(t, int32(0), actual)
						assert.Equal(t, int8(12), int8(actual.(int32)))
					},
				},
				{
					fieldName: "int16_field",
					assertFn: func(t *testing.T, actual interface{}) {
						require.IsType(t, int32(0), actual)
						assert.Equal(t, int16(1234), int16(actual.(int32)))
					},
				},
				{
					fieldName: "int32_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, int32(123456), actual.(int32))
					},
				},
				{
					fieldName: "int64_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, int64(1234567890), actual.(int64))
					},
				},
				{
					fieldName: "uint8_field",
					assertFn: func(t *testing.T, actual interface{}) {
						require.IsType(t, uint32(0), actual)
						assert.Equal(t, uint8(200), uint8(actual.(uint32)))
					},
				},
				{
					fieldName: "uint16_field",
					assertFn: func(t *testing.T, actual interface{}) {
						require.IsType(t, uint32(0), actual)
						assert.Equal(t, uint16(60000), uint16(actual.(uint32)))
					},
				},
				{
					fieldName: "uint32_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, uint32(3000000000), actual.(uint32))
					},
				},
				{
					fieldName: "uint64_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, uint64(1234567890123456789), actual.(uint64))
					},
				},
				{
					fieldName: "float32_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.InDelta(t, 3.14, float64(actual.(float32)), 0.001)
					},
				},
				{
					fieldName: "float64_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.InDelta(t, 6.28, actual.(float64), 0.001)
					},
				},
				{
					fieldName: "string_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, "hello", actual.(string))
					},
				},
				{
					fieldName: "large_string_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, "world", actual.(string))
					},
				},
				{
					fieldName: "binary_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, []byte{0x01, 0x02}, actual.([]byte))
					},
				},
				{
					fieldName: "large_binary_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, []byte{0x03, 0x04, 0x05}, actual.([]byte))
					},
				},
				{
					fieldName: "date32_field",
					assertFn: func(t *testing.T, actual interface{}) {
						// Check that the date value is non-zero.
						assert.NotZero(t, actual.(int32))
					},
				},
				{
					fieldName: "date64_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.NotZero(t, actual.(int64))
					},
				},
				{
					fieldName: "time32_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, int32(12345), actual.(int32))
					},
				},
				{
					fieldName: "time64_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, int64(67890), actual.(int64))
					},
				},
				/*
					{
						fieldName: "timestamp_field",
						assertFn: func(t *testing.T, actual interface{}) {
							if sc.useWellKnownTimestamps {
								// Expect RFC3339 string.
								tsStr := actual.(string)
								parsed, err := time.Parse(time.RFC3339, tsStr)
								require.NoError(t, err, "failed to parse well-known timestamp")
								assert.Equal(t, now.Unix(), parsed.Unix())
							} else {
								require.IsType(t, int64(0), actual)
								assert.Equal(t, int64(ts), actual.(int64))
							}
						},
					},
				*/
				{
					fieldName: "duration_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, int64(5000000000), actual.(int64))
					},
				},
				{
					fieldName: "decimal128_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, "12345.67", actual.(string))
					},
				},
				{
					fieldName: "decimal256_field",
					assertFn: func(t *testing.T, actual interface{}) {
						assert.Equal(t, "98765432.1012", actual.(string))
					},
				},
			}

			// Run each field check as a subtest.
			for _, tc := range tests {
				tc := tc // capture loop variable
				t.Run(tc.fieldName, func(t *testing.T) {
					t.Parallel()
					val := getFieldValue(tc.fieldName)
					tc.assertFn(t, val)
				})
			}
		})
	}
}
