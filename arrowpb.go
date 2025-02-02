// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

// Package arrowpb provides utilities for converting Arrow data to Protocol Buffers.
// It includes functions for generating FileDescriptorProtos, compiling them, and
// converting Arrow records to Protocol Buffer messages.

package arrowpb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ----------------------------------------------------------------------------
// 1) Package-Level Logger
// ----------------------------------------------------------------------------

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
}

// ----------------------------------------------------------------------------
// 2) ConvertConfig with Enhanced Options
// ----------------------------------------------------------------------------

// ConvertConfig allows fine-grained control over Arrow => Protobuf schema generation.
type ConvertConfig struct {
	UseWellKnownTimestamps bool // arrow.TIMESTAMP => google.protobuf.Timestamp
	UseProto2Syntax        bool // changes FileDescriptorProto syntax to "proto2"
	UseWrapperTypes        bool // arrow scalars => google.protobuf.*Value
	MapDictionariesToEnums bool // dictionary => enum

	// DescriptorCache can store repeated schemas => reuse of the same descriptor
	DescriptorCache sync.Map
}

// defaultTypeMappings returns base arrow->proto mapping, factoring in well-known timestamps.
func defaultTypeMappings(cfg *ConvertConfig) map[arrow.Type]descriptorpb.FieldDescriptorProto_Type {
	out := map[arrow.Type]descriptorpb.FieldDescriptorProto_Type{
		// Boolean
		arrow.BOOL: descriptorpb.FieldDescriptorProto_TYPE_BOOL,

		// Signed integers
		arrow.INT8:  descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT16: descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT32: descriptorpb.FieldDescriptorProto_TYPE_INT32,
		arrow.INT64: descriptorpb.FieldDescriptorProto_TYPE_INT64,

		// Unsigned integers
		arrow.UINT8:  descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT16: descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT32: descriptorpb.FieldDescriptorProto_TYPE_UINT32,
		arrow.UINT64: descriptorpb.FieldDescriptorProto_TYPE_UINT64,

		// Floating point numbers
		arrow.FLOAT16: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, // No native half-precision; map to double
		arrow.FLOAT32: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
		arrow.FLOAT64: descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,

		// Strings
		arrow.STRING:       descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.LARGE_STRING: descriptorpb.FieldDescriptorProto_TYPE_STRING,

		// Binary data
		arrow.BINARY:       descriptorpb.FieldDescriptorProto_TYPE_BYTES,
		arrow.LARGE_BINARY: descriptorpb.FieldDescriptorProto_TYPE_BYTES,

		// Date/Time types as strings (or use well-known types if desired)
		arrow.DATE32:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.DATE64:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.TIME32:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.TIME64:    descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.TIMESTAMP: descriptorpb.FieldDescriptorProto_TYPE_STRING, // will be overridden below if UseWellKnownTimestamps
		arrow.DURATION:  descriptorpb.FieldDescriptorProto_TYPE_STRING,

		// Decimal types mapped to strings (custom handling could be added if needed)
		arrow.DECIMAL128: descriptorpb.FieldDescriptorProto_TYPE_STRING,
		arrow.DECIMAL256: descriptorpb.FieldDescriptorProto_TYPE_STRING,
	}
	if cfg.UseWellKnownTimestamps {
		out[arrow.TIMESTAMP] = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	}
	return out
}

// ----------------------------------------------------------------------------
// 3) Descriptor Generation (Lists, Structs, Dictionary => Enum, Wrappers, etc.)
// ----------------------------------------------------------------------------

func GenerateUniqueMessageName(prefix string) string {
	var builder strings.Builder
	builder.WriteString(prefix)
	builder.WriteString("_")
	builder.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	builder.WriteString("_")
	builder.WriteString(fmt.Sprintf("%08d", rand.Int31()))
	return builder.String()
}

func ArrowSchemaToFileDescriptorProto(schema *arrow.Schema, packageName, messagePrefix string, cfg *ConvertConfig) (*descriptorpb.FileDescriptorProto, error) {
	if cfg == nil {
		cfg = &ConvertConfig{}
	}

	// 1) If cached
	if val, ok := cfg.DescriptorCache.Load(schema); ok {
		if fdp, ok2 := val.(*descriptorpb.FileDescriptorProto); ok2 {
			logger.Info("Using cached descriptor for schema.")
			return fdp, nil
		}
	}

	// 2) Build top-level message
	topMsgName := GenerateUniqueMessageName(messagePrefix)
	topDesc := &descriptorpb.DescriptorProto{Name: proto.String(topMsgName)}

	var nestedEnums []*descriptorpb.EnumDescriptorProto

	// 3) Build fields
	for i, f := range schema.Fields() {
		fd, moNested, moEnum, err := buildFieldDescriptor(topDesc, f, int32(i+1), cfg)
		if err != nil {
			return nil, err
		}
		topDesc.Field = append(topDesc.Field, fd)
		// moNested => nested messages
		topDesc.NestedType = append(topDesc.NestedType, moNested...)
		// moEnum => any new enums from dictionary
		nestedEnums = append(nestedEnums, moEnum...)
	}

	// Attach any newly built enums (e.g. from dictionary) to topDesc
	if len(nestedEnums) > 0 {
		topDesc.EnumType = append(topDesc.EnumType, nestedEnums...)
	}

	// 4) Build FileDescriptorProto
	syntax := "proto3"
	if cfg.UseProto2Syntax {
		syntax = "proto2"
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String(topMsgName + ".proto"),
		Package: proto.String(packageName),
		MessageType: []*descriptorpb.DescriptorProto{
			topDesc,
		},
		Syntax: proto.String(syntax),
	}

	// 5) If we reference WKT => add dependency
	//    This helps the compiler see "google/protobuf/timestamp.proto" or "wrappers.proto"
	// Add dependency for well-known timestamp if requested.
	if cfg.UseWellKnownTimestamps {
		tsDep := "google/protobuf/timestamp.proto"
		if !contains(fdp.Dependency, tsDep) {
			fdp.Dependency = append(fdp.Dependency, tsDep)
		}
	}

	// Add dependency for wrappers if requested.
	if cfg.UseWrapperTypes {
		wrapDep := "google/protobuf/wrappers.proto"
		if !contains(fdp.Dependency, wrapDep) {
			fdp.Dependency = append(fdp.Dependency, wrapDep)
		}
	}

	// 6) Store in cache
	cfg.DescriptorCache.Store(schema, fdp)
	return fdp, nil
}

func contains(deps []string, dep string) bool {
	for _, d := range deps {
		if d == dep {
			return true
		}
	}
	return false
}

// buildFieldDescriptor constructs a single field, returning:
// - FieldDescriptorProto
// - slice of nested descriptor messages
// - slice of nested enums
func buildFieldDescriptor(
	parentMsg *descriptorpb.DescriptorProto,
	field arrow.Field,
	index int32,
	cfg *ConvertConfig,
) (*descriptorpb.FieldDescriptorProto, []*descriptorpb.DescriptorProto, []*descriptorpb.EnumDescriptorProto, error) {

	fd := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(field.Name),
		Number: proto.Int32(index),
	}
	nestedMsgs := []*descriptorpb.DescriptorProto{}
	nestedEnums := []*descriptorpb.EnumDescriptorProto{}

	// 1) Struct => nested message
	if st, ok := field.Type.(*arrow.StructType); ok {
		nestedName := fmt.Sprintf("%s_struct_%d", field.Name, index)
		desc, enums, err := arrowStructToDescriptor(nestedName, st, cfg)
		if err != nil {
			return nil, nil, nil, err
		}
		fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
		fd.TypeName = proto.String(nestedName)
		nestedMsgs = append(nestedMsgs, desc)
		nestedEnums = append(nestedEnums, enums...)
		return fd, nestedMsgs, nestedEnums, nil
	}

	// 2) List => repeated
	switch lt := field.Type.(type) {
	case *arrow.ListType:
		fd.Label = descriptorpb.FieldDescriptorProto_Label(descriptorpb.FieldDescriptorProto_LABEL_REPEATED).Enum()
		elemField := arrow.Field{Name: field.Name + "_elem", Type: lt.Elem()}
		elemFD, moNested, moEnum, err := buildFieldDescriptor(parentMsg, elemField, index, cfg)
		if err != nil {
			return nil, nil, nil, err
		}
		fd.Type = elemFD.Type
		fd.TypeName = elemFD.TypeName
		nestedMsgs = append(nestedMsgs, moNested...)
		nestedEnums = append(nestedEnums, moEnum...)
		return fd, nestedMsgs, nestedEnums, nil

	case *arrow.LargeListType:
		fd.Label = descriptorpb.FieldDescriptorProto_Label(descriptorpb.FieldDescriptorProto_LABEL_REPEATED).Enum()
		elemField := arrow.Field{Name: field.Name + "_elem", Type: lt.Elem()}
		elemFD, moNested, moEnum, err := buildFieldDescriptor(parentMsg, elemField, index, cfg)
		if err != nil {
			return nil, nil, nil, err
		}
		fd.Type = elemFD.Type
		fd.TypeName = elemFD.TypeName
		nestedMsgs = append(nestedMsgs, moNested...)
		nestedEnums = append(nestedEnums, moEnum...)
		return fd, nestedMsgs, nestedEnums, nil

	case *arrow.FixedSizeListType:
		fd.Label = descriptorpb.FieldDescriptorProto_Label(descriptorpb.FieldDescriptorProto_LABEL_REPEATED).Enum()
		elemField := arrow.Field{Name: field.Name + "_elem", Type: lt.Elem()}
		elemFD, moNested, moEnum, err := buildFieldDescriptor(parentMsg, elemField, index, cfg)
		if err != nil {
			return nil, nil, nil, err
		}
		fd.Type = elemFD.Type
		fd.TypeName = elemFD.TypeName
		nestedMsgs = append(nestedMsgs, moNested...)
		nestedEnums = append(nestedEnums, moEnum...)
		return fd, nestedMsgs, nestedEnums, nil
	}

	// 3) Dictionary => maybe enum
	if dt, ok := field.Type.(*arrow.DictionaryType); ok {
		if cfg.MapDictionariesToEnums {
			// Build an enum inside the top-level message
			enumName := fmt.Sprintf("%s_dictEnum_%d", field.Name, index)
			// The field references: "ParentMsgName.enumName"
			parentName := parentMsg.GetName()
			fullEnumRef := fmt.Sprintf("%s.%s", parentName, enumName)

			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_ENUM).Enum()
			fd.TypeName = &fullEnumRef

			// Build an EnumDescriptorProto
			enumDesc := &descriptorpb.EnumDescriptorProto{
				Name: proto.String(enumName),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{
						Name:   proto.String("UNDEFINED"),
						Number: proto.Int32(0),
					},
				},
			}
			// We'll attach it as a nested enum to `parentMsg` later. But we can just return it here.
			nestedEnums = append(nestedEnums, enumDesc)
			return fd, nil, nestedEnums, nil
		}
		// otherwise fallback to dictionary's ValueType
		eqField := arrow.Field{Name: field.Name, Type: dt.ValueType}
		return buildFieldDescriptor(parentMsg, eqField, index, cfg)
	}

	// 4) Basic arrow => proto
	// 4) Basic arrow => proto
	tmap := defaultTypeMappings(cfg)
	protoType, ok := tmap[field.Type.ID()]
	if !ok {
		return nil, nil, nil, fmt.Errorf("unsupported Arrow type: %v", field.Type)
	}
	fd.Type = protoType.Enum()

	if field.Type.ID() == arrow.TIMESTAMP && cfg.UseWellKnownTimestamps {
		fd.TypeName = proto.String(".google.protobuf.Timestamp")
	}
	// Apply wrapper types if enabled.
	if cfg.UseWrapperTypes {
		switch protoType {
		case descriptorpb.FieldDescriptorProto_TYPE_INT32:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.Int32Value")
		case descriptorpb.FieldDescriptorProto_TYPE_INT64:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.Int64Value")
		case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.UInt32Value")
		case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.UInt64Value")
		case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.BoolValue")
		case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.DoubleValue")
		case descriptorpb.FieldDescriptorProto_TYPE_STRING:
			fd.Type = descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_TYPE_MESSAGE).Enum()
			fd.TypeName = proto.String(".google.protobuf.StringValue")
		}
	}

	return fd, nil, nil, nil
}

// arrowStructToDescriptor builds a nested DescriptorProto for struct fields
// also collecting any enums from dictionary subfields
func arrowStructToDescriptor(name string, st *arrow.StructType, cfg *ConvertConfig) (*descriptorpb.DescriptorProto, []*descriptorpb.EnumDescriptorProto, error) {
	desc := &descriptorpb.DescriptorProto{Name: proto.String(name)}
	var nestedEnums []*descriptorpb.EnumDescriptorProto

	for i, f := range st.Fields() {
		fd, moNested, moEnum, err := buildFieldDescriptor(desc, f, int32(i+1), cfg)
		if err != nil {
			return nil, nil, err
		}
		desc.Field = append(desc.Field, fd)
		desc.NestedType = append(desc.NestedType, moNested...)
		nestedEnums = append(nestedEnums, moEnum...)
	}
	return desc, nestedEnums, nil
}

// ----------------------------------------------------------------------------
// 4) File Compilation (Include WKT Descriptors)
// ----------------------------------------------------------------------------

// Global lazy init of well-known file descriptors
var (
	wellKnownOnce sync.Once
	wellKnownFDS  *descriptorpb.FileDescriptorSet
)

// mustLoadWellKnownTypes builds a FileDescriptorSet containing the descriptors
// for google.protobuf.Timestamp, google.protobuf.Wrappers, etc.
func mustLoadWellKnownTypes() *descriptorpb.FileDescriptorSet {
	wellKnownOnce.Do(func() {
		w1 := protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto)
		w2 := protodesc.ToFileDescriptorProto(wrapperspb.File_google_protobuf_wrappers_proto)
		wellKnownFDS = &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{w1, w2}}
	})
	return wellKnownFDS
}

// CompileFileDescriptorProto merges your generated FileDescriptorProto
// with the WKT descriptors so that references to google.protobuf.Timestamp, etc., can be resolved.
func CompileFileDescriptorProto(fdp *descriptorpb.FileDescriptorProto) (protoreflect.FileDescriptor, error) {
	// 1) Start with WKT descriptors
	allFiles := &descriptorpb.FileDescriptorSet{}
	allFiles.File = append(allFiles.File, mustLoadWellKnownTypes().File...)

	// 2) Add your own descriptor
	allFiles.File = append(allFiles.File, fdp)

	// 3) Now compile
	files, err := protodesc.NewFiles(allFiles)
	if err != nil {
		logger.Error("failed to build file descriptors",
			zap.Error(err),
			zap.String("file_name", fdp.GetName()))
		return nil, fmt.Errorf("failed to build file descriptors: %w", err)
	}
	fd, err := files.FindFileByPath(fdp.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to find file descriptor by path: %w", err)
	}
	return fd, nil
}

// CompileFileDescriptorProtoWithRetry wraps the above in an exponential backoff.
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

// GetTopLevelMessageDescriptor fetches the first message from a compiled FileDescriptor.
func GetTopLevelMessageDescriptor(fd protoreflect.FileDescriptor) (protoreflect.MessageDescriptor, error) {
	if fd.Messages().Len() == 0 {
		return nil, errors.New("file descriptor has no top-level messages")
	}
	return fd.Messages().Get(0), nil
}

// ----------------------------------------------------------------------------
// 5) Arrow => Proto Conversions
// ----------------------------------------------------------------------------

func ExtractArrowValue(col arrow.Array, rowIndex int) interface{} {
	if col.IsNull(rowIndex) {
		return nil
	}
	// If list => gather slice
	switch arr := col.(type) {
	case *array.List:
		start := arr.Offsets()[rowIndex]
		end := arr.Offsets()[rowIndex+1]
		length := end - start
		child := arr.ListValues()
		result := make([]interface{}, 0, length)
		for i := start; i < end; i++ {
			if child.IsNull(int(i)) {
				result = append(result, nil)
			} else {
				result = append(result, ExtractArrowValue(child, int(i)))
			}
		}
		return result
	case *array.LargeList:
		start := arr.Offsets()[rowIndex]
		end := arr.Offsets()[rowIndex+1]
		length := end - start
		child := arr.ListValues()
		result := make([]interface{}, 0, length)
		for i := start; i < end; i++ {
			if child.IsNull(int(i)) {
				result = append(result, nil)
			} else {
				result = append(result, ExtractArrowValue(child, int(i)))
			}
		}
		return result
	case *array.FixedSizeList:
		child := arr.ListValues()
		size := arr.Len()
		offset := rowIndex * size
		result := make([]interface{}, 0, size)
		for i := 0; i < size; i++ {
			idx := offset + i
			if idx < 0 || idx >= child.Len() {
				result = append(result, nil)
			} else if child.IsNull(idx) {
				result = append(result, nil)
			} else {
				result = append(result, ExtractArrowValue(child, idx))
			}
		}
		return result
	case *array.Dictionary:
		// Return the integer index for now
		return arr.GetValueIndex(rowIndex)
	case *array.Struct:
		// handled in handleNestedStruct
		return nil
	}
	return extractScalarValue(col, rowIndex)
}

// Basic scalar extraction
func extractScalarValue(col arrow.Array, rowIndex int) interface{} {
	switch arr := col.(type) {
	// Boolean
	case *array.Boolean:
		return arr.Value(rowIndex)

	// Signed integers: convert INT8 and INT16 to int32; INT32 remains; INT64 as is.
	case *array.Int8:
		return int32(arr.Value(rowIndex))
	case *array.Int16:
		return int32(arr.Value(rowIndex))
	case *array.Int32:
		return arr.Value(rowIndex)
	case *array.Int64:
		return arr.Value(rowIndex)

	// Unsigned integers: convert UINT8 and UINT16 to uint32; UINT32 remains; UINT64 as is.
	case *array.Uint8:
		return uint32(arr.Value(rowIndex))
	case *array.Uint16:
		return uint32(arr.Value(rowIndex))
	case *array.Uint32:
		return arr.Value(rowIndex)
	case *array.Uint64:
		return arr.Value(rowIndex)

	// Floating point numbers: convert FLOAT32 to float64 and leave FLOAT64 as is.
	case *array.Float32:
		return float64(arr.Value(rowIndex))
	case *array.Float64:
		return arr.Value(rowIndex)

	// Strings: both standard and large strings.
	case *array.String:
		return arr.Value(rowIndex)
	case *array.LargeString:
		return arr.Value(rowIndex)

	// Binary types: both standard and large.
	case *array.Binary:
		return arr.Value(rowIndex)
	case *array.LargeBinary:
		return arr.Value(rowIndex)

	// Date/Time types: preserve timezone
	case *array.Date32:
		t := arrow.Date32(arr.Value(rowIndex)).ToTime()
		// Convert to CST
		return t.In(time.FixedZone("CST", -6*3600)).Format(time.RFC3339)
	case *array.Date64:
		t := arrow.Date64(arr.Value(rowIndex)).ToTime()
		// Convert to CST
		return t.In(time.FixedZone("CST", -6*3600)).Format(time.RFC3339)
	case *array.Time32:
		return fmt.Sprintf("%v", arr.Value(rowIndex))
	case *array.Time64:
		return fmt.Sprintf("%v", arr.Value(rowIndex))
	case *array.Timestamp:
		t := arr.Value(rowIndex).ToTime(arrow.Microsecond)
		return t

	// Duration: return a formatted string.
	case *array.Duration:
		return fmt.Sprintf("%v", arr.Value(rowIndex))

	// Decimals: return a string representation with proper scale
	case *array.Decimal128:
		val := arr.Value(rowIndex)
		typ := arr.DataType().(*arrow.Decimal128Type)
		return val.ToString(typ.Scale)
	case *array.Decimal256:
		val := arr.Value(rowIndex)
		typ := arr.DataType().(*arrow.Decimal256Type)
		return val.ToString(typ.Scale)
	}
	return nil
}

// handleNestedStruct => build a nested dynamic message
func handleNestedStruct(parentMsg *dynamicpb.Message, fd protoreflect.FieldDescriptor, structArr *array.Struct, rowIndex int, cfg *ConvertConfig) error {
	if structArr.IsNull(rowIndex) {
		return nil
	}
	nestedMsg := dynamicpb.NewMessage(fd.Message())
	for i := 0; i < structArr.NumField(); i++ {
		subFD := fd.Message().Fields().Get(i)
		col := structArr.Field(i)
		val := ExtractArrowValue(col, rowIndex)
		setDynamicField(nestedMsg, subFD, val, cfg)
	}
	parentMsg.Set(fd, protoreflect.ValueOfMessage(nestedMsg))
	return nil
}

// setDynamicField => repeated fields, wrapper types, etc.
func setDynamicField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, val interface{}, cfg *ConvertConfig) {
	if val == nil {
		return
	}
	if fd.IsList() {
		slice, ok := val.([]interface{})
		if !ok {
			return
		}
		list := msg.NewField(fd).List()
		for _, elem := range slice {
			v := convertToProtoValue(fd, elem, cfg)
			if v.IsValid() {
				list.Append(v)
			}
		}
		msg.Set(fd, protoreflect.ValueOfList(list))
		return
	}
	protoVal := convertToProtoValue(fd, val, cfg)
	if protoVal.IsValid() {
		msg.Set(fd, protoVal)
	}
}

// convertToProtoValue handles well-known timestamps, wrappers, etc.
func convertToProtoValue(fd protoreflect.FieldDescriptor, val interface{}, cfg *ConvertConfig) protoreflect.Value {
	if fd.Kind() == protoreflect.MessageKind {
		fullName := string(fd.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			switch v := val.(type) {
			case time.Time:
				ts := timestamppb.New(v)
				return protoreflect.ValueOfMessage(ts.ProtoReflect())
			case string:
				if parsed, err := time.Parse(time.RFC3339, v); err == nil {
					ts := timestamppb.New(parsed)
					return protoreflect.ValueOfMessage(ts.ProtoReflect())
				}
			}
			return protoreflect.Value{}

		case "google.protobuf.Int64Value":
			if i, ok := val.(int64); ok {
				wrap := wrapperspb.Int64(i)
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			return protoreflect.Value{}

		case "google.protobuf.UInt64Value":
			if u, ok := val.(uint64); ok {
				wrap := wrapperspb.UInt64(u)
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			return protoreflect.Value{}

		case "google.protobuf.Int32Value":
			if i, ok := val.(int32); ok {
				wrap := wrapperspb.Int32(i)
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			switch v := val.(type) {
			case int8:
				wrap := wrapperspb.Int32(int32(v))
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			case int16:
				wrap := wrapperspb.Int32(int32(v))
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			return protoreflect.Value{}

		case "google.protobuf.UInt32Value":
			if u, ok := val.(uint32); ok {
				wrap := wrapperspb.UInt32(u)
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			switch v := val.(type) {
			case uint8:
				wrap := wrapperspb.UInt32(uint32(v))
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			case uint16:
				wrap := wrapperspb.UInt32(uint32(v))
				return protoreflect.ValueOfMessage(wrap.ProtoReflect())
			}
			return protoreflect.Value{}

		case "google.protobuf.BoolValue":
			b, ok := val.(bool)
			if !ok {
				return protoreflect.Value{}
			}
			wrap := wrapperspb.Bool(b)
			return protoreflect.ValueOfMessage(wrap.ProtoReflect())

		case "google.protobuf.StringValue":
			s, ok := val.(string)
			if !ok {
				return protoreflect.Value{}
			}
			wrap := wrapperspb.String(s)
			return protoreflect.ValueOfMessage(wrap.ProtoReflect())

		case "google.protobuf.DoubleValue":
			f, ok := val.(float64)
			if !ok {
				return protoreflect.Value{}
			}
			wrap := wrapperspb.Double(f)
			return protoreflect.ValueOfMessage(wrap.ProtoReflect())

		default:
			// possibly nested struct or dictionary enum
			return protoreflect.Value{}
		}
	}

	switch fd.Kind() {
	case protoreflect.BoolKind:
		if b, ok := val.(bool); ok {
			return protoreflect.ValueOfBool(b)
		}
	case protoreflect.Int64Kind:
		if i, ok := val.(int64); ok {
			return protoreflect.ValueOfInt64(i)
		}
	case protoreflect.DoubleKind:
		if f, ok := val.(float64); ok {
			return protoreflect.ValueOfFloat64(f)
		}
	case protoreflect.StringKind:
		if s, ok := val.(string); ok {
			return protoreflect.ValueOfString(s)
		}
	case protoreflect.BytesKind:
		if b, ok := val.([]byte); ok {
			return protoreflect.ValueOfBytes(b)
		}
	case protoreflect.EnumKind:
		// dictionary => enum => val is int index
		switch x := val.(type) {
		case int:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(x))
		case int64:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(x))
		}
	case protoreflect.Uint64Kind:
		if u, ok := val.(uint64); ok {
			return protoreflect.ValueOfUint64(u)
		}
		logger.Debug("uint64 conversion failed",
			zap.Any("value", val),
			zap.String("type", fmt.Sprintf("%T", val)))
	}
	return protoreflect.Value{}
}

// ----------------------------------------------------------------------------
// 6) Converting Arrow Records
// ----------------------------------------------------------------------------

func RowToDynamicProto(record arrow.Record, msgDesc protoreflect.MessageDescriptor, rowIndex int, cfg *ConvertConfig) (*dynamicpb.Message, error) {
	if record == nil || msgDesc == nil {
		return nil, errors.New("record/msgDesc cannot be nil")
	}
	if rowIndex < 0 || rowIndex >= int(record.NumRows()) {
		return nil, fmt.Errorf("rowIndex %d out of bounds", rowIndex)
	}
	dynMsg := dynamicpb.NewMessage(msgDesc)
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		fd := msgDesc.Fields().Get(colIdx)
		col := record.Column(colIdx)
		if structArr, ok := col.(*array.Struct); ok {
			if err := handleNestedStruct(dynMsg, fd, structArr, rowIndex, cfg); err != nil {
				return nil, err
			}
		} else {
			val := ExtractArrowValue(col, rowIndex)
			setDynamicField(dynMsg, fd, val, cfg)
		}
	}
	return dynMsg, nil
}

func RecordToDynamicProtos(rec arrow.Record, msgDesc protoreflect.MessageDescriptor, cfg *ConvertConfig) ([][]byte, error) {
	start := time.Now()
	rowCount := int(rec.NumRows())
	out := make([][]byte, 0, rowCount)

	for row := 0; row < rowCount; row++ {
		dyn, err := RowToDynamicProto(rec, msgDesc, row, cfg)
		if err != nil {
			return nil, err
		}
		data, err := proto.Marshal(dyn)
		if err != nil {
			return nil, err
		}
		out = append(out, data)
	}
	logger.Info("converted record to protos",
		zap.Int("row_count", rowCount),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()))
	return out, nil
}

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
	}
	return all, reader.Err()
}

// ConvertInParallel processes a single Arrow Record in parallel, chunking row ranges.
func ConvertInParallel(ctx context.Context, record arrow.Record, msgDesc protoreflect.MessageDescriptor, concurrency int, cfg *ConvertConfig) ([][]byte, error) {
	rowCount := int(record.NumRows())
	if concurrency < 1 {
		concurrency = 1
	}
	chunkSize := (rowCount + concurrency - 1) / concurrency

	results := make([][]byte, rowCount)
	var wg sync.WaitGroup
	var mu sync.Mutex
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
		go func(st, en int) {
			defer wg.Done()
			localBuf := make([][]byte, 0, en-st)
			for row := st; row < en; row++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				dyn, err := RowToDynamicProto(record, msgDesc, row, cfg)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				data, err := proto.Marshal(dyn)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				localBuf = append(localBuf, data)
			}
			mu.Lock()
			for i, b := range localBuf {
				results[st+i] = b
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

// ----------------------------------------------------------------------------
// 7) Example Utility Functions
// ----------------------------------------------------------------------------

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
	defer rec.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	return rdr, nil
}

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
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading records: %w", err)
	}

	out, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	_, err = output.Write(out)
	return err
}

func getWrappedValue(field protoreflect.FieldDescriptor, msg protoreflect.Message) interface{} {
	val := msg.Get(field)
	if field.Kind() == protoreflect.MessageKind && val.Message().IsValid() {
		fullName := string(field.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			return val.Message().Interface().(*timestamppb.Timestamp).AsTime()
		case "google.protobuf.StringValue", "google.protobuf.Int32Value",
			"google.protobuf.Int64Value", "google.protobuf.UInt32Value",
			"google.protobuf.UInt64Value", "google.protobuf.DoubleValue",
			"google.protobuf.BoolValue":
			// All wrapper types have a "value" field
			valueField := val.Message().Descriptor().Fields().ByName("value")
			if valueField != nil {
				return val.Message().Get(valueField).Interface()
			}
		}
	}
	return val.Interface()
}
