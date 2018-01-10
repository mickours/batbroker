package main

// this broker enables connection from batsim to on HPC and one Big Data
// Analytics (BDA) schedulers like presented here:
//
//                      /--- HPC
// BATSIM --- BROKER --
//                      \--- BDA
//
// Workload is split using the ``_hpc.json`` or ``_bda.json``suffix on
// the workload filename given to batsim.
//
// It also implements bebida prolog/epilog

import (
	zmq "github.com/pebbe/zmq4"
	//"time"
	"encoding/json"
	"fmt"
	//"os"
	"math"
	"strings"
)

// Declare this as global to use only two message buffer for the whole code
var jmsg BatMessage
var msg []byte
var err error

// Message from the Batsim protocole
type BatMessage struct {
	Now    float64 `json:"now"`
	Events []Event `json:"events"`
}

type Event struct {
	Timestamp float64                `json:"timestamp"`
	Type      string                 `json:"type"`
	Data      map[string]interface{} `json:"data"`
}

// Helpers
func is_bda_workload(workload_path string) bool {
	return strings.HasSuffix(workload_path, "bda.json")
}

func is_hpc_workload(workload_path string) bool {
	return strings.HasSuffix(workload_path, "hpc.json")
}

func getWorkloadID(event Event) string {
	return strings.Split(event.Data["job_id"].(string), "!")[0]
}

func recvBatsimMessage(socket *zmq.Socket) ([]byte, BatMessage) {
	msg, err = socket.RecvBytes(0)
	if err != nil {
		panic("Error while receiving Batsim message: " + err.Error())
	}

	// reset message structure
	jmsg = BatMessage{}

	if err := json.Unmarshal(msg, &jmsg); err != nil {
		panic(err)
	}
	return msg, jmsg
}

// Implement Bebida
//
// prolog
func runHPCJobProlog(now *float64, events []Event, bda_sock *zmq.Socket) {
	// Inspect HPC response
	for _, event := range events {
		switch event.Type {
		case "EXECUTE_JOB":
			{
				// Trigger HPC job prolog here
				// Ask BDA to remove allocated resources

				jmsg = BatMessage{
					Now: *now,
					Events: []Event{
						Event{
							Timestamp: *now,
							Type:      "REMOVE_RESOURCES",
							Data:      map[string]interface{}{"alloc": event.Data["alloc"]},
						},
					},
				}
				msg, err = json.Marshal(jmsg)
				// send
				bda_sock.SendBytes(msg, 0)
				// Receive acknowledgement
				_, jmsg = recvBatsimMessage(bda_sock)
				// Modify HPC execute job message's timestamp
				event.Timestamp = jmsg.Now
				// Update simulation time
				*now = jmsg.Now
			}
		}
	}
}

// epilog
func runHPCJobEpilog(now *float64, event Event, bda_sock *zmq.Socket) {
	// Give back the allocated resources to BDA
	jmsg = BatMessage{
		Now: *now,
		Events: []Event{
			Event{
				Timestamp: *now,
				Type:      "ADD_RESOURCES",
				Data:      map[string]interface{}{"alloc": event.Data["alloc"]},
			},
		},
	}
	msg, err = json.Marshal(jmsg)
	// send
	bda_sock.SendBytes(msg, 0)
	// Receive acknowledgement
	_, jmsg = recvBatsimMessage(bda_sock)

	// Modify HPC execute job message's timestamp
	event.Timestamp = jmsg.Now
	// Update simulation time
	*now = jmsg.Now
}

func main() {
	bat_host := "127.0.0.1"
	bat_port := "28000"
	bat_sock := NewReplySocket(bat_host, bat_port)
	defer bat_sock.Close()

	hpc_host := "127.0.0.1"
	hpc_port := "28001"
	hpc_sock := NewRequestSocket(hpc_host, hpc_port)
	defer hpc_sock.Close()

	bda_host := "127.0.0.1"
	bda_port := "28002"
	bda_sock := NewRequestSocket(bda_host, bda_port)
	defer bda_sock.Close()

	hpc_workload := "Not found"
	bda_workload := "Not found"

	var bda_reply BatMessage
	var hpc_reply BatMessage
	var bda_events []Event
	var hpc_events []Event
	var common_events []Event
	var now float64
	var err error
	this_is_the_end := false

	// main loop
	for !this_is_the_end {
		// clean structures
		hpc_events = []Event{}
		bda_events = []Event{}
		common_events = []Event{}
		jmsg = BatMessage{}
		bda_reply = BatMessage{}
		hpc_reply = BatMessage{}

		msg, err = bat_sock.RecvBytes(0)
		if err != nil {
			panic("Error while receiving Batsim message: " + err.Error())
		}

		if err := json.Unmarshal(msg, &jmsg); err != nil {
			panic(err)
		}
		fmt.Println("Received batsim message:\n", jmsg)

		// BATSIM --> BROKER
		now = jmsg.Now
		for _, event := range jmsg.Events {
			fmt.Println("BATSIM EVENT: ", event.Type)
			switch event.Type {
			case "JOB_SUBMITTED":
				{
					// Split message events using workload id
					switch getWorkloadID(event) {
					case hpc_workload:
						{
							hpc_events = append(hpc_events, event)
						}
					case bda_workload:
						{
							bda_events = append(bda_events, event)
						}
					}

				}
			case "SIMULATION_BEGINS":
				{
					fmt.Println("Hello Batsim!")
					// get workload/scheduler mapping
					for id, path := range event.Data["workloads"].(map[string]interface{}) {
						if is_hpc_workload(path.(string)) {
							hpc_workload = id

						} else if is_bda_workload(path.(string)) {
							bda_workload = id
						}
					}
					fmt.Println("HPC Workload id is: ", hpc_workload)
					fmt.Println("BDA Workload id is: ", bda_workload)
					common_events = append(common_events, event)
				}
			case "JOB_COMPLETED":
				{
					// Split message events using workload id
					switch getWorkloadID(event) {
					case hpc_workload:
						{
							// manage HPC jobs epilog here
							runHPCJobEpilog(&now, event, bda_sock)
							hpc_events = append(hpc_events, event)
						}
					case bda_workload:
						{
							bda_events = append(bda_events, event)
						}
					}
				}
			case "SIMULATION_ENDS":
				{
					fmt.Println("Bye Bye!")
					common_events = append(common_events, event)
					this_is_the_end = true
				}
			}
		}

		// Forward the message to one scheduler or both depending on the workload id
		// And receive response from both (send empty event if nothing to send
		// to sync time)
		//
		//            /--- HPC
		// BROKER --
		//            \--- BDA

		fmt.Println("Forwarding Batsim events to HPC scheduler")
		// merge HPC specific events and common events
		hpc_events = append(hpc_events, common_events...)
		// create the message
		msg, err = json.Marshal(BatMessage{Now: now, Events: hpc_events})
		// send
		hpc_sock.SendBytes(msg, 0)
		// get reply
		_, hpc_reply = recvBatsimMessage(hpc_sock)
		fmt.Println("Received message from HPC:\n", hpc_reply)

		// Run Bebida HPC job prolog
		runHPCJobProlog(&now, hpc_reply.Events, bda_sock)

		fmt.Println("Forwarding Batsim events to BDA scheduler")
		// merge BDA specific events and common events
		bda_events = append(bda_events, common_events...)
		// create the message
		msg, err = json.Marshal(BatMessage{Now: now, Events: bda_events})
		// send
		bda_sock.SendBytes(msg, 0)
		// get reply
		_, bda_reply = recvBatsimMessage(bda_sock)
		fmt.Println("Received message from BDA:\n", bda_reply)

		// Merge and forward message to batsim
		//
		// BATSIM <--- BROKER

		// reset message structure
		jmsg = BatMessage{}
		// merge messages with ordered events
		jmsg.Events = append(hpc_reply.Events, bda_reply.Events...)

		// get higher timestamp
		jmsg.Now = math.Max(bda_reply.Now, hpc_reply.Now)

		msg, err = json.Marshal(jmsg)
		if err != nil {
			panic("Error in message merging: " + err.Error())
		}

		fmt.Println("Forward message to Batsim:\n", jmsg)
		bat_sock.SendBytes(msg, 0)
	}
}
