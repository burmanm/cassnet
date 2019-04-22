package main

import (
	"encoding/binary"

	"github.com/burmanm/cassnet/cassandra"
	"github.com/tidwall/evio"
)

// CassandraHeaderLength is len(Header)
const CassandraHeaderLength = 9

func main() {
	ev := evio.Events{}

	ev.Opened = func(ec evio.Conn) (out []byte, opts evio.Options, action evio.Action) {
		ec.SetContext(&evio.InputStream{})
		return
	}

	ev.Data = func(c evio.Conn, in []byte) (out []byte, action evio.Action) {
		if in == nil {
			return nil, evio.Close
		}
		is := c.Context().(*evio.InputStream)
		data := is.Begin(in)
		for {
			frame := decodeFrame(data)
			if frame == nil {
				break
			}
			if len(data) > CassandraHeaderLength+int(frame.Header.Length) {
				// Too much data
				return nil, evio.Close
			}
			// Otherwise, we have received everything..
			out = processFrame(out, frame)
		}
		is.End(data)
		return
	}

	if err := evio.Serve(ev, "tcp://localhost:9042"); err != nil {
		panic(err.Error())
	}
}

// TODO Underneath should be in some other package

func decodeFrame(data []byte) *cassandra.Frame {
	if len(data) < CassandraHeaderLength {
		// Header is not ready yet
		return nil
	}
	// Parse header here, although we don't really need other than streamLength yet
	version := uint8(data[0])
	flags := uint8(data[1])
	// streamId := *(*uint16)(unsafe.Pointer(&data))
	// streamId := uint16(data[1:2])
	streamID := binary.BigEndian.Uint16(data[2:3])
	opCode := cassandra.Opcode(data[4])
	streamLength := binary.BigEndian.Uint32(data[5:8])

	if len(data) < CassandraHeaderLength+int(streamLength) {
		// We haven't received all the data yet
		return nil
	}

	header := &cassandra.Header{
		Version: version,
		Flags:   flags,
		Stream:  streamID,
		Opcode:  opCode,
	}

	return &cassandra.Frame{
		Header: header,
		Body:   data[9:],
	}
}

func processFrame(out []byte, frame *cassandra.Frame) []byte {
	// Return response-message
	// streamId from input -> output message

	var response *cassandra.Frame

	switch frame.Header.Opcode {
	case cassandra.STARTUP:
		// Body has parameter "CQL_VERSION" with value "3.0.0", we don't parse it here
		// COMPRESSION is also irrelevant to use, since we're not parsing the Body in this example
		response = cassandra.ReadyMessage(frame)
	case cassandra.PREPARE:
		// process prepare..
		response = cassandra.PreparedMessage(frame)
	// case cassandra.EXECUTE:
	// EXECUTE isn't implemented as we reply to the "INSERT" messages only in this case
	// 	// return VoidMessage
	// case cassandra.QUERY:
	// 	// return VoidMessage
	// case cassandra.REGISTER:
	// 	// return VoidMessage
	default:
		// return VoidMessage
		response = cassandra.VoidMessage(frame)
	}

	out = append(out, encodeFrame(response)...)
	return out
}

func encodeFrame(frame *cassandra.Frame) []byte {
	// These are not the most efficient way of doing this, improve after making sure this works
	encoded := make([]byte, 0, CassandraHeaderLength+len(frame.Body))
	encoded = append(encoded, frame.Header.Version)
	encoded = append(encoded, frame.Header.Flags)
	binary.BigEndian.PutUint16(encoded, frame.Header.Stream)
	encoded = append(encoded, uint8(frame.Header.Opcode))
	binary.BigEndian.PutUint32(encoded, frame.Header.Length)
	encoded = append(encoded, frame.Body...)

	return encoded
}
