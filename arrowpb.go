package arrowpb

import (
	arrowpb "github.com/TFMV/arrowpb/proto"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ConvertArrowRecord converts an Arrow RecordBatch into BigQuery Protobuf rows
func ConvertArrowRecord(record arrow.Record) ([]*arrowpb.Row, error) {
	rows := make([]*arrowpb.Row, record.NumRows())

	for i := 0; i < int(record.NumRows()); i++ {
		row := &arrowpb.Row{}

		// Extract ID (Int64)
		idCol := record.Column(0).(*array.Int64)
		if idCol.IsValid(i) {
			row.Id = idCol.Value(i)
		}

		// Extract Name (String)
		nameCol := record.Column(1).(*array.String)
		if nameCol.IsValid(i) {
			row.Name = nameCol.Value(i)
		}

		// Extract Score (Float64)
		scoreCol := record.Column(2).(*array.Float64)
		if scoreCol.IsValid(i) {
			row.Score = scoreCol.Value(i)
		}

		// Extract CreatedAt (Timestamp)
		tsCol := record.Column(3).(*array.Timestamp)
		if tsCol.IsValid(i) {
			row.CreatedAt = timestamppb.New(tsCol.Value(i).ToTime(arrow.Nanosecond))
		}

		rows[i] = row
	}

	return rows, nil
}

// SerializeRows serializes BigQuery Rows into Protobuf bytes
func SerializeRows(rows []*arrowpb.Row) ([][]byte, error) {
	out := make([][]byte, len(rows))
	for i, row := range rows {
		data, err := proto.Marshal(row)
		if err != nil {
			return nil, err
		}
		out[i] = data
	}
	return out, nil
}
