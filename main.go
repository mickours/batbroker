package main

import (
	"gitlab.inria.fr/batsim/batbrocker/message"
	"time"
)

func main() {
	host := "localhost"
	port := "5555"
	req_sock := message.NewRequestSocket(host, port)
	rep_sock := message.NewReplySocket(host, port)
	go message.SendRequest(req_sock)
	go message.SendReply(rep_sock)
	time.Sleep(50 * time.Microsecond)
}
