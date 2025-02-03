// arrowpb.go
//
// Package arrowpb provides utilities for converting Apache Arrow schemas and
// records into Protobuf descriptors and dynamic messages. These conversions
// are used to serialize Arrow record batches into Protobuf messages for writing
// to BigQuery via the Storage Write API. The conversions support options such as
// using well-known timestamp types, wrapper types (e.g. google.protobuf.StringValue),
// and proto2/proto3 syntax. For BigQuery the generated descriptor must match the
// Arrow IPC serialization.
//
// NOTE: Some BigQuery–specific value conversions (e.g. converting an Arrow value
// to a BigQuery Value) are expected to live in a separate package.
package arrowpb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cenkalti/backoff/v4"
	"github.com/stoewer/go-strcase"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// -----------------------------------------------------------------------------
// Logging & Configuration
// -----------------------------------------------------------------------------

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
}

// ConvertConfig holds options that control how an Arrow schema is converted
// into a Protobuf descriptor.
type ConvertConfig struct {
	// UseWellKnownTimestamps instructs the conversion to map Arrow TIMESTAMP
	// types to the well-known google.protobuf.Timestamp type.
	UseWellKnownTimestamps bool

	// UseProto2Syntax instructs the conversion to generate proto2 instead of proto3.
	UseProto2Syntax bool

	// UseWrapperTypes instructs the conversion to represent basic types as wrapper
	// messages (e.g. google.protobuf.StringValue) rather than primitive fields.
	UseWrapperTypes bool

	// MapDictionariesToEnums, when true, maps Arrow dictionary types to enum fields.
	MapDictionariesToEnums bool

	// ForceFlatSchema, when true, forces nested struct fields to be flattened.
	ForceFlatSchema bool

	// DescriptorCache is used to cache FileDescriptorProto values for a given Arrow schema.
	DescriptorCache sync.Map
}

// -----------------------------------------------------------------------------
// Schema Conversion: Arrow to Protobuf Descriptor
// -----------------------------------------------------------------------------

// defaultTypeMappings returns a mapping from Arrow types to the corresponding
// proto field types. The mapping may be adjusted based on the configuration.
func defaultTypeMappings(cfg *ConvertConfig) map[arrow.Type]descriptorpb.FieldDescriptorProto_Type {
	out := map[arrow.Type]descriptorpb.FieldDescriptorProto_Type{
		arrow.BOOL:         descriptorpb.FieldDescriptorProto_TYPE_BOOL,
		arrow.INT8:         descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT16:        descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT32:        descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT64:        descriptorpb.FieldDescriptorProto_TYPE_INT64,
		arrow.UINT8:        descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT16:       descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT32:       descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT64:       descriptorpb.FieldDescriptorProto_TYPE_UINT64,
		arrow.FLOAT32:      descriptorpb.FieldDescriptorProto_TYPE_FLOAT,
		arrow.FLOAT64:      descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
		arrow.STRING:       descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.LARGE_STRING: descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.BINARY:       descriptorpb.FieldDescriptorProto_TYPE_BYTES,
		arrow.LARGE_BINARY: descriptorpb.FieldDescriptorProto_TYPE_BYTES,
		arrow.DATE32:       descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.DATE64:       descriptorpb.FieldDescriptorProto_TYPE_INT64,
		arrow.TIME32:       descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.TIME64:       descriptorpb.FieldDescriptorProto_TYPE_INT64,
		arrow.TIMESTAMP:    descriptorpb.FieldDescriptorProto_TYPE_INT64,
		arrow.DURATION:     descriptorpb.FieldDescriptorProto_TYPE_INT64,

		// Decimals are converted to strings.
		arrow.DECIMAL128: descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.DECIMAL256: descriptorpb.FieldDescriptorProto_TYPE_STRING,

		// Complex types (struct, list, map, etc.) are represented as messages (or enum for dictionary).
		arrow.STRUCT:          descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		arrow.LIST:            descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		arrow.LARGE_LIST:      descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		arrow.FIXED_SIZE_LIST: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		arrow.DICTIONARY:      descriptorpb.FieldDescriptorProto_TYPE_ENUM,
		arrow.MAP:             descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
	}

	// Override TIMESTAMP mapping if using well-known timestamps.
	if cfg.UseWellKnownTimestamps {
		out[arrow.TIMESTAMP] = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	}
	return out
}

// ArrowSchemaToFileDescriptorProto converts an Arrow schema to a FileDescriptorProto.
// The generated proto descriptor (and top-level message) can be compiled and used to
// dynamically serialize Arrow record batches for BigQuery.
func ArrowSchemaToFileDescriptorProto(schema *arrow.Schema, packageName, messagePrefix string, cfg *ConvertConfig) (*descriptorpb.FileDescriptorProto, error) {
	if cfg == nil {
		cfg = &ConvertConfig{}
	}

	// Check cache first.
	if val, ok := cfg.DescriptorCache.Load(schema); ok {
		if fdp, ok2 := val.(*descriptorpb.FileDescriptorProto); ok2 {
			logger.Info("Using cached descriptor for schema.")
			return fdp, nil
		}
	}

	// Generate a unique top-level message name.
	topMsgName := fmt.Sprintf("%s_%d", messagePrefix, time.Now().UnixNano())
	topDesc := &descriptorpb.DescriptorProto{
		Name: proto.String(topMsgName),
	}

	// Build field descriptors for each field in the Arrow schema.
	for i, f := range schema.Fields() {
		fd, _, _, err := buildFieldDescriptor(f, int32(i+1), cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to build descriptor for field %q: %w", f.Name, err)
		}
		topDesc.Field = append(topDesc.Field, fd)
	}

	syntax := "proto3"
	if cfg.UseProto2Syntax {
		syntax = "proto2"
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(topMsgName + ".proto"),
		Package:     proto.String(packageName),
		MessageType: []*descriptorpb.DescriptorProto{topDesc},
		Syntax:      proto.String(syntax),
	}

	// Add dependencies if wrapper or well-known types are used.
	if cfg.UseWrapperTypes {
		fdp.Dependency = append(fdp.Dependency, "google/protobuf/wrappers.proto")
	}
	if cfg.UseWellKnownTimestamps {
		fdp.Dependency = append(fdp.Dependency, "google/protobuf/timestamp.proto")
	}

	cfg.DescriptorCache.Store(schema, fdp)
	logger.Debug("Generated Proto Descriptor", zap.String("descriptor", protojson.Format(fdp)))
	return fdp, nil
}

// buildFieldDescriptor converts a single Arrow field into a FieldDescriptorProto.
// For dictionary and struct types (if not forced flat), special handling is applied.
func buildFieldDescriptor(field arrow.Field, index int32, cfg *ConvertConfig) (
	*descriptorpb.FieldDescriptorProto,
	[]*descriptorpb.DescriptorProto, // nested messages (if any)
	[]*descriptorpb.EnumDescriptorProto, // nested enums (if any)
	error,
) {
	fieldName := strcase.SnakeCase(field.Name)
	fd := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(fieldName),
		Number: proto.Int32(index),
	}

	// If the field is a dictionary and dictionaries should be mapped to enums.
	if _, ok := field.Type.(*arrow.DictionaryType); ok && cfg.MapDictionariesToEnums {
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()
		fd.TypeName = proto.String(fmt.Sprintf("%s_enum", fieldName))
		return fd, nil, nil, nil
	}

	// For struct types, if not forcing a flat schema, treat as a nested message.
	if _, ok := field.Type.(*arrow.StructType); ok && !cfg.ForceFlatSchema {
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fd.TypeName = proto.String(fmt.Sprintf("%s_struct", fieldName))
		return fd, nil, nil, nil
	}

	// Map basic types.
	tmap := defaultTypeMappings(cfg)
	protoType, ok := tmap[field.Type.ID()]
	if !ok {
		return nil, nil, nil, fmt.Errorf("unsupported Arrow type: %v", field.Type)
	}
	fd.Type = protoType.Enum()

	// For decimal types, no further changes.
	if field.Type.ID() == arrow.DECIMAL128 || field.Type.ID() == arrow.DECIMAL256 {
		return fd, nil, nil, nil
	}

	// For TIMESTAMP, if using well-known timestamps.
	if field.Type.ID() == arrow.TIMESTAMP && cfg.UseWellKnownTimestamps {
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fd.TypeName = proto.String("google.protobuf.Timestamp")
		return fd, nil, nil, nil
	}

	// If using wrapper types, update the type and type name accordingly.
	if cfg.UseWrapperTypes {
		switch fd.GetType() {
		case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.BoolValue")
		case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.UInt32Value")
		case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.UInt64Value")
		case descriptorpb.FieldDescriptorProto_TYPE_STRING:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.StringValue")
		case descriptorpb.FieldDescriptorProto_TYPE_INT32:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.Int32Value")
		case descriptorpb.FieldDescriptorProto_TYPE_INT64:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.Int64Value")
		case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.FloatValue")
		case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.DoubleValue")
		case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = proto.String("google.protobuf.BytesValue")
		}
	}

	return fd, nil, nil, nil
}

// CompileFileDescriptorProto builds a protoreflect.FileDescriptor from the given
// FileDescriptorProto. It also attempts to load well-known types if referenced.
func CompileFileDescriptorProto(fdp *descriptorpb.FileDescriptorProto) (protoreflect.FileDescriptor, error) {
	fdset := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fdp},
	}

	// If the descriptor references google.protobuf types, try to load them.
	if strings.Contains(protojson.Format(fdp), "google.protobuf") {
		wkts := []string{
			"google/protobuf/wrappers.proto",
			"google/protobuf/timestamp.proto",
		}
		for _, wkt := range wkts {
			if wktFile, err := protoregistry.GlobalFiles.FindFileByPath(wkt); err == nil {
				fdset.File = append(fdset.File, protodesc.ToFileDescriptorProto(wktFile))
			} else {
				logger.Error("Failed to load well-known type", zap.String("file", wkt), zap.Error(err))
			}
		}
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

// CompileFileDescriptorProtoWithRetry attempts to compile the file descriptor proto
// and retries with exponential backoff for up to 3 seconds.
func CompileFileDescriptorProtoWithRetry(fdp *descriptorpb.FileDescriptorProto) (protoreflect.FileDescriptor, error) {
	var out protoreflect.FileDescriptor
	op := func() error {
		fd, err := CompileFileDescriptorProto(fdp)
		out = fd
		return err
	}
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 3 * time.Second
	if err := backoff.Retry(op, b); err != nil {
		return nil, err
	}
	return out, nil
}

// GetTopLevelMessageDescriptor retrieves the first (top-level) message descriptor
// from a FileDescriptor.
func GetTopLevelMessageDescriptor(fd protoreflect.FileDescriptor) (protoreflect.MessageDescriptor, error) {
	if fd.Messages().Len() == 0 {
		return nil, errors.New("file descriptor has no top-level messages")
	}
	return fd.Messages().Get(0), nil
}

// -----------------------------------------------------------------------------
// Dynamic Message Conversion: Arrow Record → Dynamic Proto Message
// -----------------------------------------------------------------------------

// ExtractArrowValue extracts the underlying value from an Arrow array at the given index.
func ExtractArrowValue(col arrow.Array, rowIndex int) interface{} {
	if col == nil || rowIndex < 0 || rowIndex >= col.Len() {
		return nil
	}
	switch arr := col.(type) {
	case *array.Boolean:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Int8:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Int16:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Int32:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Int64:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Uint8:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Uint16:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Uint32:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Uint64:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Float32:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Float64:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.String:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Binary:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Timestamp:
		if arr.IsValid(rowIndex) {
			if tsType, ok := arr.DataType().(*arrow.TimestampType); ok {
				ts := arr.Value(rowIndex)
				return ts.ToTime(tsType.Unit).UnixMicro()
			}
		}
	case *array.Date32:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex).ToTime()
		}
	case *array.Date64:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex).ToTime()
		}
	case *array.Time32:
		if arr.IsValid(rowIndex) {
			return arrow.Time32(arr.Value(rowIndex))
		}
	case *array.Time64:
		if arr.IsValid(rowIndex) {
			return arrow.Time64(arr.Value(rowIndex))
		}
	case *array.Duration:
		if arr.IsValid(rowIndex) {
			return arrow.Duration(arr.Value(rowIndex))
		}
	case *array.List:
		if arr.IsValid(rowIndex) {
			var list []interface{}
			listArr := arr.ListValues()
			start, end := arr.ValueOffsets(rowIndex)
			for i := start; i < end; i++ {
				list = append(list, ExtractArrowValue(listArr, int(i)))
			}
			return list
		}
	case *array.LargeString:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.LargeBinary:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Decimal128:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	case *array.Decimal256:
		if arr.IsValid(rowIndex) {
			return arr.Value(rowIndex)
		}
	}
	return nil
}

// setDynamicField sets the value of a field in a dynamic proto message.
// It handles repeated and map fields as well as wrapper messages.
func setDynamicField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, val interface{}, cfg *ConvertConfig) error {
	if fd == nil || msg == nil {
		return errors.New("field descriptor or message cannot be nil")
	}

	// Handle null values.
	if val == nil {
		if fd.IsList() || fd.IsMap() {
			msg.Set(fd, protoreflect.ValueOf(msg.NewField(fd).List()))
		} else {
			msg.Clear(fd)
		}
		return nil
	}

	// Repeated fields.
	if fd.IsList() {
		list := msg.Mutable(fd).List()
		valSlice, ok := val.([]interface{})
		if !ok {
			return fmt.Errorf("expected slice for repeated field %s, got %T", fd.Name(), val)
		}
		for _, v := range valSlice {
			if fd.Kind() == protoreflect.MessageKind {
				wrapperMsg := dynamicpb.NewMessage(fd.Message())
				if err := setDynamicWrapper(wrapperMsg, v); err != nil {
					return fmt.Errorf("failed to set list wrapper field %s: %w", fd.Name(), err)
				}
				list.Append(protoreflect.ValueOf(wrapperMsg))
			} else {
				protoVal, err := convertToProtoValue(v, fd, cfg)
				if err != nil {
					return fmt.Errorf("failed to convert list item for %s: %w", fd.Name(), err)
				}
				list.Append(protoVal)
			}
		}
		return nil
	}

	// Map fields.
	if fd.IsMap() {
		mapField := msg.Mutable(fd).Map()
		valMap, ok := val.(map[string]interface{})
		if !ok {
			return fmt.Errorf("expected map for field %s, got %T", fd.Name(), val)
		}
		for k, v := range valMap {
			keyVal, err := convertToProtoValue(k, fd.MapKey(), cfg)
			if err != nil {
				return fmt.Errorf("failed to convert map key for %s: %w", fd.Name(), err)
			}
			var valueVal protoreflect.Value
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				wrapperMsg := dynamicpb.NewMessage(fd.MapValue().Message())
				if err := setDynamicWrapper(wrapperMsg, v); err != nil {
					return fmt.Errorf("failed to set map wrapper field %s: %w", fd.Name(), err)
				}
				valueVal = protoreflect.ValueOf(wrapperMsg)
			} else {
				valueVal, err = convertToProtoValue(v, fd.MapValue(), cfg)
				if err != nil {
					return fmt.Errorf("failed to convert map value for %s: %w", fd.Name(), err)
				}
			}
			mapField.Set(protoreflect.ValueOf(keyVal.String()).MapKey(), valueVal)
		}
		return nil
	}

	// For message kind (i.e. wrappers).
	if fd.Kind() == protoreflect.MessageKind {
		wrapperMsg := dynamicpb.NewMessage(fd.Message())
		if err := setDynamicWrapper(wrapperMsg, val); err != nil {
			return fmt.Errorf("failed to set wrapper field %s: %w", fd.Name(), err)
		}
		msg.Set(fd, protoreflect.ValueOf(wrapperMsg))
		return nil
	}

	protoVal, err := convertToProtoValue(val, fd, cfg)
	if err != nil {
		return fmt.Errorf("failed to convert field %s: %w", fd.Name(), err)
	}
	msg.Set(fd, protoVal)
	return nil
}

// setDynamicWrapper sets the underlying "value" field of a wrapper message.
func setDynamicWrapper(msg *dynamicpb.Message, val interface{}) error {
	// Check the message type first
	switch msg.Descriptor().FullName() {
	case "google.protobuf.Timestamp":
		// Get the seconds and nanos fields
		secondsField := msg.Descriptor().Fields().ByName("seconds")
		nanosField := msg.Descriptor().Fields().ByName("nanos")
		if secondsField == nil || nanosField == nil {
			return errors.New("timestamp message missing seconds/nanos fields")
		}

		var t time.Time
		switch v := val.(type) {
		case time.Time:
			t = v
		case int64:
			t = time.UnixMicro(v)
		case arrow.Timestamp:
			t = v.ToTime(arrow.Microsecond)
		default:
			return fmt.Errorf("cannot convert %T to timestamp", val)
		}

		msg.Set(secondsField, protoreflect.ValueOfInt64(t.Unix()))
		msg.Set(nanosField, protoreflect.ValueOfInt32(int32(t.Nanosecond())))
		return nil
	}

	// Handle other wrapper types normally
	valueField := msg.Descriptor().Fields().ByName("value")
	if valueField == nil {
		return errors.New("wrapper message missing 'value' field")
	}
	protoVal, err := convertToProtoValue(val, valueField, &ConvertConfig{})
	if err != nil {
		return fmt.Errorf("failed to convert value for wrapper: %w", err)
	}
	msg.Set(valueField, protoVal)
	return nil
}

// convertToProtoValue converts a Go value into a protoreflect.Value appropriate for the field.
func convertToProtoValue(val interface{}, field protoreflect.FieldDescriptor, cfg *ConvertConfig) (protoreflect.Value, error) {
	if val == nil {
		return protoreflect.Value{}, nil
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		if v, ok := val.(bool); ok {
			return protoreflect.ValueOfBool(v), nil
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		switch v := val.(type) {
		case int8:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case int16:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case int32:
			return protoreflect.ValueOfInt32(v), nil
		case int64:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case int:
			return protoreflect.ValueOfInt32(int32(v)), nil
		case time.Time:
			// For date32 fields, convert time.Time to days since epoch
			return protoreflect.ValueOfInt32(int32(arrow.Date32FromTime(v))), nil
		case arrow.Time32:
			return protoreflect.ValueOfInt32(int32(v)), nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		switch v := val.(type) {
		case uint8:
			return protoreflect.ValueOfUint32(uint32(v)), nil
		case uint16:
			return protoreflect.ValueOfUint32(uint32(v)), nil
		case uint32:
			return protoreflect.ValueOfUint32(v), nil
		case uint64:
			return protoreflect.ValueOfUint32(uint32(v)), nil
		case uint:
			return protoreflect.ValueOfUint32(uint32(v)), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		switch v := val.(type) {
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case int32:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case time.Time:
			// For date64 fields, convert time.Time to milliseconds since epoch
			return protoreflect.ValueOfInt64(int64(arrow.Date64FromTime(v))), nil
		case arrow.Time64:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case arrow.Duration:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case arrow.Timestamp:
			return protoreflect.ValueOfInt64(int64(v)), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		switch v := val.(type) {
		case uint64:
			return protoreflect.ValueOfUint64(v), nil
		case uint32:
			return protoreflect.ValueOfUint64(uint64(v)), nil
		}
	case protoreflect.FloatKind:
		switch v := val.(type) {
		case float32:
			return protoreflect.ValueOfFloat32(v), nil
		case float64:
			return protoreflect.ValueOfFloat32(float32(v)), nil
		}
	case protoreflect.DoubleKind:
		switch v := val.(type) {
		case float64:
			return protoreflect.ValueOfFloat64(v), nil
		case float32:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		}
	case protoreflect.StringKind:
		switch v := val.(type) {
		case string:
			return protoreflect.ValueOfString(v), nil
		case *array.String:
			return protoreflect.ValueOfString(v.ValueStr(0)), nil
		case decimal128.Num:
			return protoreflect.ValueOfString(v.ToString(2)), nil
		case decimal256.Num:
			return protoreflect.ValueOfString(v.ToString(4)), nil
		}
	case protoreflect.BytesKind:
		switch v := val.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(v), nil
		case *array.Binary:
			return protoreflect.ValueOfBytes(v.Value(0)), nil
		}
	case protoreflect.EnumKind:
		switch v := val.(type) {
		case int32:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
		case int64:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
		case string:
			enumVal := field.Enum().Values().ByName(protoreflect.Name(v))
			if enumVal != nil {
				return protoreflect.ValueOfEnum(enumVal.Number()), nil
			}
			return protoreflect.Value{}, fmt.Errorf("invalid enum value %v", v)
		}
	case protoreflect.MessageKind:
		fullName := string(field.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			switch v := val.(type) {
			case time.Time:
				return protoreflect.ValueOfString(v.Format(time.RFC3339)), nil
			case int64:
				t := time.UnixMicro(v)
				return protoreflect.ValueOfString(t.Format(time.RFC3339)), nil
			case arrow.Timestamp:
				t := v.ToTime(arrow.Microsecond)
				return protoreflect.ValueOfString(t.Format(time.RFC3339)), nil
			}
		case "google.protobuf.Int64Value":
			if v, ok := val.(int64); ok {
				return protoreflect.ValueOfMessage(wrapperspb.Int64(v).ProtoReflect()), nil
			}
		}
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported field kind: %v", field.Kind())
	}
	return protoreflect.Value{}, fmt.Errorf("type mismatch: cannot convert %T to %v", val, field.Kind())
}

// handleNestedStruct handles converting a nested Arrow struct column into a dynamic proto message.
func handleNestedStruct(dynMsg *dynamicpb.Message, fd protoreflect.FieldDescriptor, structArr *array.Struct, rowIndex int, cfg *ConvertConfig) error {
	if dynMsg == nil || fd == nil || structArr == nil {
		return errors.New("message, field descriptor, or struct array is nil")
	}
	if rowIndex < 0 || rowIndex >= structArr.Len() {
		return fmt.Errorf("rowIndex %d out of bounds", rowIndex)
	}
	if !structArr.IsValid(rowIndex) {
		return nil
	}

	nestedMsg := dynamicpb.NewMessage(fd.Message())
	for i := 0; i < structArr.NumField(); i++ {
		nestedField := fd.Message().Fields().Get(i)
		col := structArr.Field(i)
		val := ExtractArrowValue(col, rowIndex)
		if err := setDynamicField(nestedMsg, nestedField, val, cfg); err != nil {
			return fmt.Errorf("failed to set nested field %s in struct %s: %w", nestedField.Name(), fd.Name(), err)
		}
	}
	dynMsg.Set(fd, protoreflect.ValueOf(nestedMsg))
	return nil
}

// RowToDynamicProto converts a single row from an Arrow record to a dynamic proto message.
func RowToDynamicProto(rec arrow.Record, msgDesc protoreflect.MessageDescriptor, rowIndex int, cfg *ConvertConfig) (*dynamicpb.Message, error) {
	if rec == nil || msgDesc == nil {
		return nil, errors.New("record and message descriptor cannot be nil")
	}
	if rowIndex < 0 || rowIndex >= int(rec.NumRows()) {
		return nil, fmt.Errorf("rowIndex %d out of bounds", rowIndex)
	}
	dynMsg := dynamicpb.NewMessage(msgDesc)
	for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
		fd := msgDesc.Fields().Get(colIdx)
		col := rec.Column(colIdx)
		// If the column is a struct, handle it specially.
		if structArr, ok := col.(*array.Struct); ok {
			if err := handleNestedStruct(dynMsg, fd, structArr, rowIndex, cfg); err != nil {
				return nil, fmt.Errorf("error handling nested struct for field %s: %w", fd.Name(), err)
			}
		} else {
			val := ExtractArrowValue(col, rowIndex)
			if err := setDynamicField(dynMsg, fd, val, cfg); err != nil {
				return nil, fmt.Errorf("failed to set field %s: %w", fd.Name(), err)
			}
		}
	}
	return dynMsg, nil
}

// RecordToDynamicProtos converts an entire Arrow record into a slice of serialized proto messages.
func RecordToDynamicProtos(rec arrow.Record, msgDesc protoreflect.MessageDescriptor, cfg *ConvertConfig) ([][]byte, error) {
	start := time.Now()
	rowCount := int(rec.NumRows())
	out := make([][]byte, 0, rowCount)

	for row := 0; row < rowCount; row++ {
		dynMsg, err := RowToDynamicProto(rec, msgDesc, row, cfg)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	logger.Info("Converted Arrow record to protos",
		zap.Int("row_count", rowCount),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	return out, nil
}

// ArrowReaderToProtos reads Arrow records from a RecordReader and converts them to serialized protos.
func ArrowReaderToProtos(ctx context.Context, reader array.RecordReader, msgDesc protoreflect.MessageDescriptor, cfg *ConvertConfig) ([][]byte, error) {
	defer reader.Release()
	var all [][]byte
	for reader.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		rec := reader.Record()
		rows, err := RecordToDynamicProtos(rec, msgDesc, cfg)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
		rec.Release()
	}
	return all, reader.Err()
}

// ConvertInParallel converts rows from an Arrow record in parallel into serialized protos.
// It uses an error group to cancel all workers upon any error.
func ConvertInParallel(ctx context.Context, rec arrow.Record, msgDesc protoreflect.MessageDescriptor, concurrency int, cfg *ConvertConfig) ([][]byte, error) {
	rowCount := int(rec.NumRows())
	if concurrency < 1 {
		concurrency = 1
	}
	chunkSize := (rowCount + concurrency - 1) / concurrency
	results := make([][]byte, rowCount)
	eg, egCtx := errgroup.WithContext(ctx)
	var mu sync.Mutex

	for w := 0; w < concurrency; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > rowCount {
			end = rowCount
		}
		if start >= end {
			break
		}
		eg.Go(func(st, en int) func() error {
			return func() error {
				localBuf := make([][]byte, 0, en-st)
				for row := st; row < en; row++ {
					select {
					case <-egCtx.Done():
						return egCtx.Err()
					default:
					}
					dynMsg, err := RowToDynamicProto(rec, msgDesc, row, cfg)
					if err != nil {
						return err
					}
					data, err := proto.Marshal(dynMsg)
					if err != nil {
						return err
					}
					localBuf = append(localBuf, data)
				}
				mu.Lock()
				for i, b := range localBuf {
					results[st+i] = b
				}
				mu.Unlock()
				return nil
			}
		}(start, end))
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// GetDynamicFieldValue extracts the underlying scalar value from a dynamic message field.
// If the field is a wrapper type (google.protobuf.*Value), it returns the inner value.
func GetDynamicFieldValue(field protoreflect.FieldDescriptor, msg protoreflect.Message) interface{} {
	val := msg.Get(field)
	if field.Kind() == protoreflect.MessageKind && val.Message().IsValid() {
		fullName := string(val.Message().Descriptor().FullName())
		if strings.HasPrefix(fullName, "google.protobuf.") {
			if valueField := val.Message().Descriptor().Fields().ByName("value"); valueField != nil {
				return val.Message().Get(valueField).Interface()
			}
		}
	}
	return val.Interface()
}

// -----------------------------------------------------------------------------
// (Optional) Testing / Utility Functions
// -----------------------------------------------------------------------------

// CreateArrowRecord is a simple helper to create an Arrow record for testing purposes.
func CreateArrowRecord() (array.RecordReader, error) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie", "Diana"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2, 76.8, 88.0}, nil)

	rec := builder.NewRecord()
	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	return rdr, nil
}

// FormatArrowJSON outputs an Arrow record as formatted JSON for debugging.
func FormatArrowJSON(reader array.RecordReader, output io.Writer) error {
	defer reader.Release()
	var rows []map[string]interface{}
	for reader.Next() {
		rec := reader.Record()
		for row := 0; row < int(rec.NumRows()); row++ {
			rowData := make(map[string]interface{})
			for colIdx, f := range rec.Schema().Fields() {
				col := rec.Column(colIdx)
				rowData[f.Name] = ExtractArrowValue(col, row)
			}
			rows = append(rows, rowData)
		}
		rec.Release()
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading records: %w", err)
	}
	outBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	_, err = output.Write(outBytes)
	return err
}
