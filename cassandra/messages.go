package cassandra

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

var prepareCount uint64

const (
	cqlShStartMessage = "SELECT * FROM system.local WHERE key='local'"
)

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
	replyBody := make([]byte, 4)
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
	// replyBody := []byte{}
	buf := new(bytes.Buffer)
	// replyBody := make([]byte, 6, 26)
	// replyBody := make([]byte, 0, 256) // TODO Add length for allocation
	binary.Write(buf, binary.BigEndian, uint32(0x0004))
	// binary.BigEndian.PutUint32(replyBody, 0x0004)
	// fmt.Printf("1: Length to encode: %d\n", len(replyBody))

	statementNumber := make([]byte, 8)
	binary.BigEndian.PutUint64(statementNumber, atomic.AddUint64(&prepareCount, 1))

	statementID := md5.Sum(statementNumber)
	// statementID := preparedHasher.Sum(statementNumber)

	// Write id, [short bytes]
	binary.Write(buf, binary.BigEndian, uint16(16))

	// binary.BigEndian.PutUint16(replyBody[4:], 16)

	// fmt.Printf("2: Length to encode: %d\n", len(replyBody))

	if len(statementID) != 16 {
		panic("StatementID is incorrect")
	}

	binary.Write(buf, binary.BigEndian, statementID)
	// replyBody = append(replyBody, statementID[:]...)

	// Write metadata - yes, it needs to be written
	prepareMeta := prepare(inputMessage.Body) // this should be created first..
	encodePreparedMetadata(buf, prepareMeta)

	// Write resultMetadata, which is the same as rows metadata..
	resultMeta := prepareMetadataToResultMetadata(prepareMeta)
	encodeResultMetadata(buf, resultMeta)

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

	// fmt.Printf("Length to encode: %d\n", len(replyBody))
	return replyMessage(RESULT, inputMessage.Header.Stream, buf.Bytes())
}

func prepare(body []byte) *PreparedMetadata {
	// Parse the body and prepare those?
	return prepareCassandraStress()
}

// prepareCassandraStress works for the normal insert workload in the cassandra-stress tool
func prepareCassandraStress() *PreparedMetadata {
	columns := make([]ColumnSpecification, 6)
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

func writeString(output io.Writer, input string) {
	result := []byte(input)
	binary.Write(output, binary.BigEndian, uint16(len(result)))
	binary.Write(output, binary.BigEndian, result)
}

// encodePreparedMetadata <flags><columns_count><pk_count>[<pk_index_1>...<pk_index_n>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
func encodePreparedMetadata(output io.Writer, metadata *PreparedMetadata) {
	// startPos := len(output)
	// fmt.Printf("3 Encoding continuing from: %d\n", startPos)
	binary.Write(output, binary.BigEndian, uint32(metadata.Flags))
	// binary.BigEndian.PutUint32(output[startPos:], uint32(metadata.Flags))          // flags
	binary.Write(output, binary.BigEndian, uint32(len(metadata.Columns)))
	// binary.BigEndian.PutUint32(output[startPos+4:], uint32(len(metadata.Columns))) // columns_count

	binary.Write(output, binary.BigEndian, uint32(len(metadata.PartitionKeyBindingIndexes)))
	// binary.BigEndian.PutUint32(output[startPos+8:], uint32(len(metadata.PartitionKeyBindingIndexes))) // pk_count

	// fmt.Printf("4 Encoding continuing from: %d\n", startPos)

	// pk_index_1 ... pk_index_n
	for _, s := range metadata.PartitionKeyBindingIndexes {
		binary.Write(output, binary.BigEndian, uint16(s))
		// binary.BigEndian.PutUint16(output[startPos:], s)
	}

	// We assume same table and keyspace for all of these.. as this is global_table_spec only
	// <global_table_spec>
	writeString(output, metadata.Columns[0].Keyspace)
	writeString(output, metadata.Columns[0].Table)

	// col_spec_1 ... col_spec_n
	for _, c := range metadata.Columns {
		writeString(output, c.ColumnName)
		// binary.Write(output, binary.BigEndian, []byte(c.ColumnName))
		// output = append(output, []byte(c.ColumnName)...)
		// Write type..

		/*
			    [option]       A pair of <id><value> where <id> is a [short] representing
			                   the option id and <value> depends on that option (and can be
			                   of size 0). The supported id (and the corresponding <value>)
							   will be described when this is used.
				BLOB     (3,  BytesType.instance, ProtocolVersion.V1),
		*/
		binary.Write(output, binary.BigEndian, uint16(c.Type))
		// binary.BigEndian.PutUint16(output, c.Type)
	}
}

func prepareMetadataToResultMetadata(preparedMeta *PreparedMetadata) *ResultMetadata {
	// This isn't correct as these flags are not correct - but we don't use them yet
	return &ResultMetadata{
		Flags:   preparedMeta.Flags,
		Columns: preparedMeta.Columns,
	}
}

func encodeResultMetadata(output io.Writer, metadata *ResultMetadata) {
	binary.Write(output, binary.BigEndian, uint32(metadata.Flags))        // flags
	binary.Write(output, binary.BigEndian, uint32(len(metadata.Columns))) // columns_count

	// <global_table_spec>
	writeString(output, metadata.Columns[0].Keyspace)
	writeString(output, metadata.Columns[0].Table)

	// col_spec_1 ... col_spec_n
	for _, c := range metadata.Columns {
		writeString(output, c.ColumnName)
		binary.Write(output, binary.BigEndian, uint16(c.Type)) // Only native types supported
	}
}

func readLongString(start uint32, body []byte) (uint32, string) {
	sizeOfString := binary.BigEndian.Uint32(body[start : start+4])
	stringEnd := start + 4 + sizeOfString
	return stringEnd, string(body[start+4 : stringEnd])
}

func QueryMessage(inputMessage *Frame) *Frame {
	/*
		     Performs a CQL query. The body of the message must be:
		       <query><query_parameters>
		     where <query> is a [long string] representing the query and
		   	<query_parameters> must be
			   <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]

			[long string] An [int] n, followed by n bytes representing an UTF-8 string.
			[int] A 4 bytes integer
	*/
	_, queryMessage := readLongString(0, inputMessage.Body)
	if queryMessage == cqlShStartMessage {
		// Process and return..
		// if SELECT * FROM system.local WHERE key='local' reply with:
		/*
						cqlsh wants these:

			            'build': result['release_version'],
			            'protocol': result['native_protocol_version'],
			            'cql': result['cql_version'],

		*/
		replyBody := make([]byte, 4)
		binary.BigEndian.PutUint32(replyBody, 0x0002) // ROWS reply
		/*
		     Indicates a set of rows. The rest of the body of a Rows result is:
		       <metadata><rows_count><rows_content>
		     where:
		       - <metadata> is composed of:
		   		 <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
		*/
		binary.BigEndian.PutUint32(replyBody, 0x0004) // No_metadata
		// encodeResultMetadata allows writing col_specs..
		binary.BigEndian.PutUint32(replyBody, 3) // Column count of 3
		binary.BigEndian.PutUint32(replyBody, 1) // Row count of 1

		//
		// [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
		// no byte should follow and the value represented is `null`.
		/*
			    - <rows_content> is composed of <row_1>...<row_m> where m is <rows_count>.
			    Each <row_i> is composed of <value_1>...<value_n> where n is
			    <columns_count> and where <value_j> is a [bytes] representing the value
			    returned for the jth column of the ith row. In other words, <rows_content>
				is composed of (<rows_count> * <columns_count>) [bytes].
		*/

	}

	return VoidMessage(inputMessage)
}
