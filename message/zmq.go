package message

import (
	zmq "github.com/pebbe/zmq4"

	"fmt"
)

func newSocket(host string, port string, zmq_socket_type zmq.Type) *zmq.Socket {
	// Create Request socket
	fmt.Println("Connecting to " + host + ":" + port)
	socket, err := zmq.NewSocket(zmq_socket_type)
	if err != nil {
		panic("Unable to create the socket" + host + ":" + port + " with error: " + err.Error())
	}
	return socket
}

// Public API

func NewRequestSocket(host string, port string) *zmq.Socket {
	requester := newSocket(host, port, zmq.REQ)
	// Connect to Reply socket
	err := requester.Connect("tcp://" + host + ":" + port)
	if err != nil {
		panic("Error while connecting to socket: " + err.Error())
	}
	return requester
}

func NewReplySocket(host string, port string) *zmq.Socket {
	responder := newSocket(host, port, zmq.REP)
	endpoint := "tcp://" + host + ":" + port
	err := responder.Bind(endpoint)
	if err != nil {
		panic("Error while binding socket to \"" + endpoint + "\" with error: " + err.Error())
	}
	return responder
}
