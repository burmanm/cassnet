package main

import (
	"encoding/binary"
	"fmt"

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
			readCount, frame := decodeFrame(data)
			if frame == nil {
				break
			}
			// fmt.Printf("Reading %d bytes out of %d\n", readCount, len(data))
			/*
				if len(data) > readCount {
					fmt.Printf("Too much data, %d > %d: %08b\n", len(data), CassandraHeaderLength+int(frame.Header.Length), data)
					// Too much data
					return nil, evio.Close
				}
			*/
			// fmt.Printf("Received: %08b\n", data[:readCount])
			data = data[readCount:]
			out = append(out, processFrame(frame)...)
			// fmt.Printf("Sending: %08b\n", out)
		}
		is.End(data)
		return
	}

	ev.Serving = func(server evio.Server) (action evio.Action) {
		fmt.Printf("Server started\n")
		for _, a := range server.Addrs {
			fmt.Printf("Listening at %s\n", a.String())
		}
		fmt.Printf("Using %d event loops\n", server.NumLoops)
		return
	}

	if err := evio.Serve(ev, "tcp://localhost:9042"); err != nil {
		panic(err.Error())
	}
}

// TODO Underneath should be in some other package

func decodeFrame(data []byte) (int, *cassandra.Frame) {
	// fmt.Printf("data len: %d\n", len(data))
	if len(data) < CassandraHeaderLength {
		// Header is not ready yet
		return 0, nil
	}
	// Parse header here, although we don't really need other than streamLength yet
	version := uint8(data[0])
	flags := uint8(data[1])
	streamID := binary.BigEndian.Uint16(data[2:4])
	opCode := cassandra.Opcode(data[4])
	streamLength := binary.BigEndian.Uint32(data[5:9])

	if len(data) < CassandraHeaderLength+int(streamLength) {
		// We haven't received all the data yet
		return 0, nil
	}

	header := &cassandra.Header{
		Version: version,
		Flags:   flags,
		Stream:  streamID,
		Opcode:  opCode,
		Length:  streamLength,
	}

	endPos := CassandraHeaderLength + int(streamLength)

	return endPos, &cassandra.Frame{
		Header: header,
		Body:   data[CassandraHeaderLength:endPos],
	}
}

func processFrame(frame *cassandra.Frame) []byte {
	// Return response-message
	// streamId from input -> output message

	var response *cassandra.Frame

	// fmt.Printf("Processing: %d\n", frame.Header.Opcode)
	switch frame.Header.Opcode {
	// case cassandra.REGISTER:
	// 	fallthrough
	case cassandra.STARTUP:
		// Body has parameter "CQL_VERSION" with value "3.0.0", we don't parse it here
		// COMPRESSION is also irrelevant to use, since we're not parsing the Body in this example
		// fmt.Printf("Processing startup: %v -> %v: %v\n", frame.Header.Stream, frame.Header.Length, string(frame.Body))
		response = cassandra.ReadyMessage(frame)
		// fmt.Printf("response: %v\n", encodeFrame(response))
	case cassandra.PREPARE:
		// process prepare..
		// fmt.Printf("Processing prepare\n")
		response = cassandra.PreparedMessage(frame)
	// case cassandra.EXECUTE:
	// EXECUTE isn't implemented as we reply to the "INSERT" messages only in this case
	// 	// return VoidMessage
	case cassandra.QUERY:
		// fmt.Printf("Received opCode: QUERY\n")
		// fmt.Printf("%v\n", string(frame.Body))
		response = cassandra.QueryMessage(frame)
		// response = cassandra.VoidMessage(frame)
	// case cassandra.REGISTER:
	// 	// return VoidMessage
	default:
		// return VoidMessage
		// fmt.Printf("Received opCode: %d\n", frame.Header.Opcode)
		response = cassandra.VoidMessage(frame)
	}

	return encodeFrame(response)
}

func encodeFrame(frame *cassandra.Frame) []byte {
	encoded := make([]byte, CassandraHeaderLength+len(frame.Body))
	encoded[0] = frame.Header.Version
	encoded[1] = frame.Header.Flags
	binary.BigEndian.PutUint16(encoded[2:4], frame.Header.Stream)
	encoded[4] = uint8(frame.Header.Opcode)
	binary.BigEndian.PutUint32(encoded[5:9], frame.Header.Length)
	copy(encoded[9:], frame.Body)

	// fmt.Printf("frame: %02b -> %02b -> %02b\n", frame.Header.Version, frame.Header.Opcode, frame.Header.Stream)
	// fmt.Printf("encoded: %02b\n", encoded)

	// fmt.Printf("Body length: %d == %d, body: %v\n", frame.Header.Length, len(frame.Body), string(frame.Body))

	return encoded
}
