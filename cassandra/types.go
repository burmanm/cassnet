package cassandra

// ProtocolVersion for V4 of Cassandra protocol
const ProtocolVersion = 0x04

// ProtocolReplyVersion for V4 of Cassandra protocol
const ProtocolReplyVersion = 0x84

type Frame struct {
	Header *Header
	Body   []byte
}

type Header struct {
	Version uint8 // 0x04 request, 0x84 reply
	Flags   uint8
	Stream  uint16
	Opcode  Opcode // Integer, uint8
	Length  uint32
}

type Opcode uint8

const (
	ERROR Opcode = iota
	STARTUP
	READY
	AUTHENTICATE
	_
	OPTIONS
	SUPPORTED
	QUERY
	RESULT
	PREPARE
	EXECUTE
	REGISTER
	EVENT
	BATCH
	AUTH_CHALLENGE
	AUTH_RESPONSE
	AUTH_SUCCESS
)

/*
Opcode to MessageType
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  UnsupportedMessageCodec.instance),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);
*/

type PreparedMetadata struct {
	Flags                      int32
	Columns                    []ColumnSpecification // It's not really []string, but this is enough for now
	PartitionKeyBindingIndexes []uint16
}

type ColumnSpecification struct {
	Keyspace   string
	Table      string
	ColumnName string
	Type       uint16 // Only support basic types
}
