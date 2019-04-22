package cassandra

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

var prepareCount uint64

func replyMessage(opCode Opcode, streamID uint16, body []byte) *Frame {
	header := &Header{
		Version: ProtocolReplyVersion,
		Flags:   0,
		Stream:  streamID,
		Opcode:  opCode,
		Length:  uint32(len(body)),
	}

	return &Frame{
		Header: header,
		Body:   body,
	}
}

// VoidMessage is an empty RESULT reply with code 0x0001 and rest of the body is empty
func VoidMessage(inputMessage *Frame) *Frame {
	replyBody := make([]byte, 0, 4)
	binary.BigEndian.PutUint32(replyBody, 0x0001)
	return replyMessage(RESULT, inputMessage.Header.Stream, replyBody)
}

// ReadyMessage is a reply to STARTUP, indicating the server is ready. Body is empty.
func ReadyMessage(inputMessage *Frame) *Frame {
	return replyMessage(READY, inputMessage.Header.Stream, make([]byte, 0))
}

// PreparedMessage returns acknowledge to prepare, the body parameter consists of the CQL query to prepare as a [long string].
// This method is currently only preparing for the cassandra-stress program as there's no real backend
func PreparedMessage(inputMessage *Frame) *Frame {
	replyBody := make([]byte, 0) // TODO Add length for allocation
	binary.BigEndian.PutUint32(replyBody, 0x0004)

	preparedHasher := md5.New()

	statementNumber := make([]byte, 0, 8)
	binary.BigEndian.PutUint64(statementNumber, atomic.AddUint64(&prepareCount, 1))
	statementID := preparedHasher.Sum(statementNumber)

	// Write id, [short bytes]
	binary.BigEndian.PutUint16(replyBody, 16)
	replyBody = append(replyBody, statementID...)

	// Write metadata
	prepareMeta := prepare(inputMessage.Body) // this should be created first..
	replyBody = encodePreparedMetadata(replyBody, prepareMeta)

	// Write resultMetadata, which is the same as rows metadata..
	resultMeta := prepareMetadataToResultMetadata(prepareMeta)
	replyBody = encodeResultMetadata(replyBody, resultMeta)

	// The result to a PREPARE message. The body of a Prepared result is:
	// [short] A 2 bytes unsigned integer
	// [short bytes] A [short] n, followed by n bytes if n >= 0.
	//
	// <id><metadata><result_metadata>
	// <id> is [short bytes] representing the prepared query ID.
	//     - <metadata> is composed of:
	//         <flags><columns_count><pk_count>[<pk_index_1>...<pk_index_n>][<global_table_spec>?<col_spec_1>...<col_spec_n>]

	/*
			For the cassandra-stress:
		                    PrepareMessage pr = (PrepareMessage) request;

		                    logger.info(pr.toString());

		                    MD5Digest statementId = MD5Digest.compute("statementId");
		                    MD5Digest resultMetadataId = MD5Digest.compute("resultMetadataId");

		                    List<ColumnSpecification> columnNames = new ArrayList<>();
		                    for (int i = 0; i < 6; i++)
		                        columnNames.add(new ColumnSpecification("keyspace1", "standard1", new ColumnIdentifier("C" + i,
		                                false), BytesType.instance));

		                    // v3 encoding doesn't include partition key bind indexes
		                    ResultSet.PreparedMetadata meta = new ResultSet.PreparedMetadata(columnNames, new short[]{ 2, 1 });
		                    ResultSet.ResultMetadata resultMeta = new ResultSet.ResultMetadata(columnNames);
		                    response = new ResultMessage.Prepared(statementId, resultMetadataId, meta, resultMeta);
	*/

	return replyMessage(RESULT, inputMessage.Header.Stream, replyBody)
}

func prepare(body []byte) *PreparedMetadata {
	// Parse the body and prepare those?
	return prepareCassandraStress()
}

// prepareCassandraStress works for the normal insert workload in the cassandra-stress tool
func prepareCassandraStress() *PreparedMetadata {
	columns := make([]ColumnSpecification, 0, 6)
	for i := 0; i < 6; i++ {
		columns[i] = ColumnSpecification{
			Keyspace:   "keyspace1",
			Table:      "standard1",
			ColumnName: fmt.Sprintf("C%d", i),
			Type:       3, // BLOB
		}
	}
	pm := &PreparedMetadata{
		Columns:                    columns,
		Flags:                      0x0001, // 0x0001 == GlobalTableSpec
		PartitionKeyBindingIndexes: []uint16{2, 1},
	}
	return pm
}

// encodePreparedMetadata <flags><columns_count><pk_count>[<pk_index_1>...<pk_index_n>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
func encodePreparedMetadata(output []byte, metadata *PreparedMetadata) []byte {
	binary.BigEndian.PutUint32(output, uint32(metadata.Flags))        // flags
	binary.BigEndian.PutUint32(output, uint32(len(metadata.Columns))) // columns_count

	binary.BigEndian.PutUint32(output, uint32(len(metadata.PartitionKeyBindingIndexes))) // pk_count

	// pk_index_1 ... pk_index_n
	for _, s := range metadata.PartitionKeyBindingIndexes {
		binary.BigEndian.PutUint16(output, s)
	}

	// We assume same table and keyspace for all of these.. as this is global_table_spec only
	// <global_table_spec>
	output = append(output, []byte(metadata.Columns[0].Keyspace)...)
	output = append(output, []byte(metadata.Columns[0].Table)...)

	// col_spec_1 ... col_spec_n
	for _, c := range metadata.Columns {
		output = append(output, []byte(c.ColumnName)...)
		// Write type..

		/*
			    [option]       A pair of <id><value> where <id> is a [short] representing
			                   the option id and <value> depends on that option (and can be
			                   of size 0). The supported id (and the corresponding <value>)
							   will be described when this is used.
				BLOB     (3,  BytesType.instance, ProtocolVersion.V1),
		*/
		binary.BigEndian.PutUint16(output, c.Type)
	}

	return output
}

func prepareMetadataToResultMetadata(preparedMeta *PreparedMetadata) *ResultMetadata {
	// This isn't correct as these flags are not correct - but we don't use them yet
	return &ResultMetadata{
		Flags:   preparedMeta.Flags,
		Columns: preparedMeta.Columns,
	}
}

func encodeResultMetadata(output []byte, metadata *ResultMetadata) []byte {
	binary.BigEndian.PutUint32(output, uint32(metadata.Flags))        // flags
	binary.BigEndian.PutUint32(output, uint32(len(metadata.Columns))) // columns_count

	// <global_table_spec>
	output = append(output, []byte(metadata.Columns[0].Keyspace)...)
	output = append(output, []byte(metadata.Columns[0].Table)...)

	// col_spec_1 ... col_spec_n
	for _, c := range metadata.Columns {
		output = append(output, []byte(c.ColumnName)...)
		binary.BigEndian.PutUint16(output, c.Type) // Only native types supported
	}

	return output
}
