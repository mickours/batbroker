package message

import (
	zmq "github.com/pebbe/zmq4"

	"fmt"
	"time"
)

func newSocket(host string, port string, zmq_socket_type zmq.Type) *zmq.Socket {
	// Create Request socket
	fmt.Println("Connecting to " + host + ":" + port)
	socket, err := zmq.NewSocket(zmq_socket_type)
	if err != nil {
		panic("Unable to create the socket" + host + ":" + port + " with error: " + err.Error())
	}
	defer socket.Close()
	return socket
}

// Public API

func NewRequestSocket(host string, port string) *zmq.Socket {
	requester := newSocket(host, port, zmq.REQ)
	// Connect to Reply socket
	requester.Connect("tcp://" + host + ":" + port)
	return requester
}

func NewReplySocket(host string, port string) *zmq.Socket {
	responder := newSocket(host, port, zmq.REP)
	responder.Bind("tcp://" + host + ":" + port)
	return responder
}

func SendRequest(requester *zmq.Socket) {
	for {
		// send hello
		msg := fmt.Sprintf("Hello %d", time.Now())
		fmt.Println("Sending ", msg)
		requester.Send(msg, 0)

		// Wait for reply:
		reply, _ := requester.Recv(0)
		fmt.Println("Received ", reply)
	}
}

func SendReply(responder *zmq.Socket) {
	for {
		//  Wait for next request from client
		msg, _ := responder.Recv(0)
		fmt.Println("Received ", msg)

		//  Do some 'work'
		time.Sleep(time.Second)

		//  Send reply back to client
		reply := "World"
		responder.Send(reply, 0)
		fmt.Println("Sent ", reply)
	}
}
