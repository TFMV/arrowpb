// fuzz_protorand_test.go

package arrowpb

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrow"
	"github.com/loicalleyne/bufarrow/gen/go/samples"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func FuzzProtoArrowRoundTrip(f *testing.F) {
	// Add some seed corpus
	f.Add(uint64(42), float64(3.14))

	f.Fuzz(func(t *testing.T, i uint64, f float64) {
		// 1. Create a test proto message
		msg := &samples.Three{
			Value: i,
		}

		logger.Debug("test input",
			zap.Uint64("original_value", i))

		// 2. Convert to Arrow using bufarrow
		schema, err := bufarrow.New[*samples.Three](memory.DefaultAllocator)
		if err != nil {
			t.Skip("failed to create schema:", err)
		}
		defer schema.Release()

		// Append the message
		schema.Append(msg)
		record := schema.NewRecord()
		defer record.Release()

		// 3. Convert back to proto using our package
		// First create a descriptor
		fdp, err := ArrowSchemaToFileDescriptorProto(record.Schema(), "test", "TestMsg", nil)
		if err != nil {
			t.Fatal("failed to create descriptor:", err)
		}

		// Compile the descriptor
		fd, err := CompileFileDescriptorProto(fdp)
		if err != nil {
			t.Fatal("failed to compile descriptor:", err)
		}

		// Get message descriptor
		msgDesc, err := GetTopLevelMessageDescriptor(fd)
		if err != nil {
			t.Fatal("failed to get message descriptor:", err)
		}

		// Convert record back to protos
		protoBytes, err := RecordToDynamicProtos(record, msgDesc, nil)
		if err != nil {
			t.Fatal("failed to convert record to protos:", err)
		}

		// 4. Verify round trip
		if len(protoBytes) != 1 {
			t.Fatal("expected 1 proto message, got:", len(protoBytes))
		}

		// Parse the dynamic proto
		dynMsg := &samples.Three{}
		if err := proto.Unmarshal(protoBytes[0], dynMsg); err != nil {
			t.Fatal("failed to unmarshal proto:", err)
		}

		// Compare fields
		if dynMsg.Value != msg.Value {
			t.Errorf("Value mismatch: got %v, want %v", dynMsg.Value, msg.Value)
		}

		logger.Debug("extracted value",
			zap.Any("value", ExtractArrowValue(record.Column(0), 0)))
	})
}

// Helper function to test parallel conversion
func TestParallelConversion(t *testing.T) {
	// Create a large test record using bufarrow
	schema, err := bufarrow.New[*samples.Three](memory.DefaultAllocator)
	if err != nil {
		t.Fatal("failed to create schema:", err)
	}
	defer schema.Release()

	// Add multiple rows
	for i := 0; i < 1000; i++ {
		msg := &samples.Three{
			Value: uint64(i),
		}
		schema.Append(msg)
	}
	record := schema.NewRecord()
	defer record.Release()

	// Convert using parallel processing
	fdp, err := ArrowSchemaToFileDescriptorProto(record.Schema(), "test", "TestMsg", nil)
	if err != nil {
		t.Fatal("failed to create descriptor:", err)
	}

	fd, err := CompileFileDescriptorProto(fdp)
	if err != nil {
		t.Fatal("failed to compile descriptor:", err)
	}

	msgDesc, err := GetTopLevelMessageDescriptor(fd)
	if err != nil {
		t.Fatal("failed to get message descriptor:", err)
	}

	ctx := context.Background()
	protos, err := ConvertInParallel(ctx, record, msgDesc, 4, nil)
	if err != nil {
		t.Fatal("parallel conversion failed:", err)
	}

	if len(protos) != 1000 {
		t.Errorf("expected 1000 protos, got %d", len(protos))
	}
}
