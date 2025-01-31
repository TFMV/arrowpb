package arrowpb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TypeMappings maps Apache Arrow types to Protocol Buffers field types.
var TypeMappings = map[arrow.DataType]descriptorpb.FieldDescriptorProto_Type{
	arrow.BinaryTypes.Binary:          descriptorpb.FieldDescriptorProto_TYPE_BYTES,
	arrow.FixedWidthTypes.Boolean:     descriptorpb.FieldDescriptorProto_TYPE_BOOL,
	arrow.PrimitiveTypes.Float64:      descriptorpb.FieldDescriptorProto_TYPE_DOUBLE,
	arrow.PrimitiveTypes.Int64:        descriptorpb.FieldDescriptorProto_TYPE_INT64,
	arrow.BinaryTypes.String:          descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.FixedWidthTypes.Date32:      descriptorpb.FieldDescriptorProto_TYPE_STRING,
	arrow.FixedWidthTypes.Timestamp_s: descriptorpb.FieldDescriptorProto_TYPE_STRING,
}

// GenerateUniqueName creates a unique name for the ProtoBuf message.
func GenerateUniqueName(prefix string) string {
	timestamp := time.Now().Unix()
	randomSuffix := rand.Intn(9999)
	return fmt.Sprintf("%s_%d_%04d", prefix, timestamp, randomSuffix)
}

// CreateNestedMessage constructs a nested ProtoBuf message descriptor.
func CreateNestedMessage(fieldType *arrow.StructType, messageName string) *descriptorpb.DescriptorProto {
	nestedDescriptor := &descriptorpb.DescriptorProto{Name: &messageName}

	for i, field := range fieldType.Fields() {
		fieldProto := &descriptorpb.FieldDescriptorProto{
			Name:   &field.Name,
			Number: proto.Int32(int32(i + 1)),
		}

		if protoType, exists := TypeMappings[field.Type]; exists {
			fieldProto.Type = &protoType
		} else {
			log.Fatalf("Unsupported nested type: %v", field.Type)
		}

		nestedDescriptor.Field = append(nestedDescriptor.Field, fieldProto)
	}
	return nestedDescriptor
}

// ArrowSchemaToProto converts an Arrow schema into a Protocol Buffers descriptor.
func ArrowSchemaToProto(schema *arrow.Schema) *descriptorpb.DescriptorProto {
	messageName := GenerateUniqueName("ArrowMessage")

	descriptorProto := &descriptorpb.DescriptorProto{Name: &messageName}

	// Process nested struct fields first
	nestedTypes := make(map[string]*descriptorpb.DescriptorProto)
	for _, field := range schema.Fields() {
		if structType, ok := field.Type.(*arrow.StructType); ok {
			nestedName := field.Name + "Type"
			nestedMessage := CreateNestedMessage(structType, nestedName)
			nestedTypes[field.Name] = nestedMessage
			descriptorProto.NestedType = append(descriptorProto.NestedType, nestedMessage)
		}
	}

	// Process primary fields
	for i, field := range schema.Fields() {
		fieldProto := &descriptorpb.FieldDescriptorProto{
			Name:   &field.Name,
			Number: proto.Int32(int32(i + 1)),
		}

		if nested, exists := nestedTypes[field.Name]; exists {
			messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
			fieldProto.Type = &messageType
			fieldProto.TypeName = nested.Name
		} else if protoType, exists := TypeMappings[field.Type]; exists {
			fieldProto.Type = &protoType
		} else {
			log.Fatalf("Unsupported Arrow type: %v", field.Type)
		}

		descriptorProto.Field = append(descriptorProto.Field, fieldProto)
	}

	return descriptorProto
}

// ArrowRecordToProto converts an Arrow RecordBatch into serialized ProtoBuf messages.
func ArrowRecordToProto(record arrow.Record, descriptorProto *descriptorpb.DescriptorProto) ([]byte, error) {
	// Initialize Protobuf message map
	protoMessage := make(map[string]interface{})

	// Iterate through all fields and extract values
	for colIndex, field := range record.Schema().Fields() {
		column := record.Column(colIndex)
		fieldName := field.Name

		// Extract value based on type
		value := ExtractValue(column, 0) // Assuming single-row records

		// Assign to the Protobuf message map
		if value != nil {
			protoMessage[fieldName] = value
		}
	}

	// Serialize the message
	return proto.Marshal(&descriptorpb.DescriptorProto{
		Name:  descriptorProto.Name,
		Field: descriptorProto.Field,
	})
}

// ExtractValue retrieves a value from an Arrow column at a specific row index.
func ExtractValue(col arrow.Array, rowIndex int) any {
	switch col := col.(type) {
	case *array.Int64:
		return col.Value(rowIndex)
	case *array.Float64:
		return col.Value(rowIndex)
	case *array.String:
		return col.Value(rowIndex)
	case *array.Boolean:
		return col.Value(rowIndex)
	default:
		return nil
	}
}

// ArrowRowToProto converts a single row in an Arrow RecordBatch to a Protobuf message.
func ArrowRowToProto(record arrow.Record, descriptorProto *descriptorpb.DescriptorProto, rowIndex int) ([]byte, error) {
	protoMessage := make(map[string]interface{})

	for colIdx, field := range record.Schema().Fields() {
		column := record.Column(colIdx)
		fieldName := field.Name

		// Extract value for the specific row
		value := ExtractValue(column, rowIndex)

		if value != nil {
			protoMessage[fieldName] = value
		}
	}

	return proto.Marshal(&descriptorpb.DescriptorProto{
		Name:  descriptorProto.Name,
		Field: descriptorProto.Field,
	})
}

// ArrowBatchToProto converts an Arrow RecordReader into serialized ProtoBuf messages.
func ArrowBatchToProto(reader array.RecordReader, protoDescriptor *descriptorpb.DescriptorProto) [][]byte {
	var protoMessages [][]byte

	for reader.Next() {
		record := reader.Record()

		for row := 0; row < int(record.NumRows()); row++ {
			message, err := ArrowRowToProto(record, protoDescriptor, row)
			if err != nil {
				log.Printf("Failed to convert row %d to Protobuf: %v", row, err)
				continue
			}
			protoMessages = append(protoMessages, message)
		}
	}

	if err := reader.Err(); err != nil {
		log.Printf("Error while reading records: %v", err)
	}

	return protoMessages
}

// CreateArrowRecord creates a sample Arrow RecordBatch for testing.
func CreateArrowRecord() (array.RecordReader, error) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// Append sample data
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)
	builder.Field(2).(*array.Float64Builder).AppendValues([]float64{95.5, 89.2, 76.8}, nil)

	record := builder.NewRecord()
	return array.NewRecordReader(schema, []arrow.Record{record})
}

// FormatArrowJSON formats Arrow records as pretty-printed JSON.
func FormatArrowJSON(reader array.RecordReader, output io.Writer) error {
	defer reader.Release()

	var records []map[string]interface{}

	for reader.Next() {
		record := reader.Record()

		for row := 0; row < int(record.NumRows()); row++ {
			rowData := make(map[string]interface{})

			for colIdx, field := range record.Schema().Fields() {
				col := record.Column(colIdx)
				rowData[field.Name] = ExtractValue(col, row)
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
