package main

import (
	//zmq "github.com/pebbe/zmq4"
	"gitlab.inria.fr/batsim/batbrocker/message"
	//"time"
	"encoding/json"
	"fmt"
	//"os"
	"strings"
)

//func main() {
//host := "localhost"
//port := "5555"
//req_sock := message.NewRequestSocket(host, port)
//rep_sock := message.NewReplySocket(host, port)
//go message.SendRequest(req_sock)
//go message.SendReply(rep_sock)
//time.Sleep(50 * time.Microsecond)
//}

type BatMessage struct {
	Now    float64
	Events []Event
}

type Event struct {
	Timestamp float64
	Type      string
	Data      map[string]interface{}
}

func main() {
	bat_host := "127.0.0.1"
	bat_port := "28000"
	bat_sock := message.NewReplySocket(bat_host, bat_port)
	defer bat_sock.Close()

	hpc_host := "127.0.0.1"
	hpc_port := "28001"
	hpc_sock := message.NewRequestSocket(hpc_host, hpc_port)
	defer hpc_sock.Close()

	//bda_host := "localhost"
	//bda_port := "5557"
	//bda_sock := message.NewRequestSocket(bda_host, bda_port)

	jmsg := BatMessage{}
	this_is_the_end := false
	// main loop
	for !this_is_the_end {
		msg, err := bat_sock.RecvBytes(0)
		if err != nil {
			panic("Error while receiving Batsim message: " + err.Error())
		}

		if err := json.Unmarshal(msg, &jmsg); err != nil {
			panic(err)
		}
		fmt.Println("Received batsim message:\n", jmsg)

		for _, event := range jmsg.Events {
			fmt.Println("BATSIM EVENT: ", event.Type)
			switch event.Type {
			case "JOB_SUBMITTED":
				{
					// get workload id
					workload_id := strings.Split(event.Data["job_id"].(string), "!")[0]
					fmt.Println("Workload id: ", workload_id)
				}
			case "SIMULATION_BEGINS":
				{
					fmt.Println("Hello!")
					// TODO get workload/schdeduler mapping (Need batsim feature)
				}
			case "JOB_COMPLETED":
				{
					// get workload id
					workload_id := strings.Split(event.Data["job_id"].(string), "!")[0]
					fmt.Println("Workload id: ", workload_id)
					// TODO manage HPC jobs epilog here
				}
			case "SIMULATION_ENDS":
				{
					fmt.Println("Bye Bye!")
					this_is_the_end = true
				}
			}
		}

		fmt.Println("Forwarding Batsim message to HPC scheduler")
		hpc_sock.SendBytes(msg, 0)
		msg, err = hpc_sock.RecvBytes(0)
		if err != nil {
			panic("Error while receiving Batsim message: " + err.Error())
		}

		if err := json.Unmarshal(msg, &jmsg); err != nil {
			panic(err)
		}

		fmt.Println("Received HPC message:\n", jmsg)
		for _, event := range jmsg.Events {
			switch event.Type {
			case "EXECUTE_JOB":
				{
					// TODO manage HPC job prolog here
				}
			default:
				fmt.Println("Not Handled HPC EVENT: ", event.Type)
			}
		}
		fmt.Println("Forward HPC message to Batsim:\n", jmsg)
		bat_sock.SendBytes(msg, 0)
	}
}
