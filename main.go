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
var hpc_reply_json []byte
var bda_reply_json []byte
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

func getWorkloadID(id string) string {
	return strings.Split(id, "!")[0]
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

func removeEvents(to_remove_indexes []int, events *[]Event) {
	// Do a reverse range to avoid index error
	last := len(to_remove_indexes)-1
	for i := range to_remove_indexes {
		reverse_i := to_remove_indexes[last - i]
		(*events) = append(
			(*events)[:reverse_i],
			(*events)[reverse_i+1:]...
		)
	}
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
	var prolog_blocked_hpc_events []Event
	// var epilog_blocked_hpc_events []Event
	var to_remove_indexes []int
	var now float64
	var err error
	this_is_the_end := false
	resumited_bda_workload := "resubmit"

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
		fmt.Println("Batsim -> Broker:\n", string(msg))

		// BATSIM --> BROKER
		// Inspect Batsim request
		now = jmsg.Now
		for _, event := range jmsg.Events {
			switch event.Type {
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
			case "JOB_SUBMITTED":
				{
					// Split message events using workload id
					switch getWorkloadID(event.Data["job_id"].(string)) {
					case hpc_workload:
						{
							hpc_events = append(hpc_events, event)
						}
					// WARN Dynamically submitted jobs are always given to BDA
					case bda_workload, resumited_bda_workload:
						{
							bda_events = append(bda_events, event)
						}
					default:
						panic("This event should go somewhere!")
					}

				}
			case "JOB_KILLED":
				{
					// Split message events using first job workload id
					// FIXME check if all jobs are from the same workload
					switch getWorkloadID(event.Data["job_ids"].([]interface{})[0].(string)) {
					case hpc_workload:
						{
							hpc_events = append(hpc_events, event)
						}
					case bda_workload, resumited_bda_workload:
						{
							bda_events = append(bda_events, event)
						}
					}

				}
			case "JOB_COMPLETED":
				{
					// Split message events using workload id
					switch getWorkloadID(event.Data["job_id"].(string)) {
					case hpc_workload:
						{
							// manage HPC jobs epilog here
							fmt.Println("Trigger HPC job epilog for resources: ", event.Data["alloc"])
							// Give back the allocated resources to BDA
							new_event := Event{
										Timestamp: now,
										Type:      "ADD_RESOURCES",
										Data:      map[string]interface{}{"resources": event.Data["alloc"]},
							}
							bda_events = append(bda_events, new_event)
							// wait for the resources to be added to the BDA resource
							// pool before notifiing the HPC scheduler that the job is
							// complete
							//epilog_blocked_hpc_events = append(epilog_blocked_hpc_events, event)
							hpc_events = append(hpc_events, event)
						}
					case bda_workload, resumited_bda_workload:
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

		// merge HPC specific events and common events
		hpc_events = append(hpc_events, common_events...)
		// create the message
		msg, err = json.Marshal(BatMessage{Now: now, Events: hpc_events})
		// send
		hpc_sock.SendBytes(msg, 0)
		fmt.Println("Broker -> HPC:\n", string(msg))

		// get reply
		hpc_reply_json, hpc_reply = recvBatsimMessage(hpc_sock)
		fmt.Println("Broker <= HPC:\n", string(hpc_reply_json))

		// Inspect HPC response
		to_remove_indexes = []int{}
		for index, event := range hpc_reply.Events {
			switch event.Type {
			case "EXECUTE_JOB":
				{
					// Trigger HPC job prolog here
					// Run Bebida HPC job prolog
					fmt.Println("Trigger HPC job prolog for resources: ", event.Data["alloc"])
					// Ask BDA to remove allocated resources
					new_event := Event{
						Timestamp: hpc_reply.Now,
						Type:      "REMOVE_RESOURCES",
						Data:      map[string]interface{}{"resources": event.Data["alloc"]},
					}
					bda_events = append(bda_events, new_event)
					prolog_blocked_hpc_events = append(prolog_blocked_hpc_events, event)
					to_remove_indexes = append(to_remove_indexes, index)
				}
			}
		}
		// Hold events by removing them from events to forward
		removeEvents(to_remove_indexes, &hpc_reply.Events)

		// merge BDA specific events and common events
		bda_events = append(bda_events, common_events...)
		// create the message
		msg, err = json.Marshal(BatMessage{Now: now, Events: bda_events})
		// send
		bda_sock.SendBytes(msg, 0)
		fmt.Println("Broker -> BDA:\n", string(msg))

		// get reply
		bda_reply_json, bda_reply = recvBatsimMessage(bda_sock)
		fmt.Println("Broker <= BDA:\n", string(bda_reply_json))

		// Inspect BDA reply
		to_remove_indexes = []int{}
		for index, event := range bda_reply.Events {
			switch event.Type {
			case "RESOURCES_REMOVED":
				{
					// End of prolog: Resource removed event from BDA so release the message
					// pop blocked event
					var to_add_event Event
					to_add_event, prolog_blocked_hpc_events = prolog_blocked_hpc_events[0], prolog_blocked_hpc_events[1:]
					// check that the removed resources are the same that are
					// allocated by the HPC job
					if to_add_event.Data["alloc"] != event.Data["resources"] {
						panic("Error in prolog ordering!!!")
					}
					// Add it to the reply
					hpc_reply.Events = append(hpc_reply.Events, to_add_event)
					// remove this event from BDA events
					to_remove_indexes = append(to_remove_indexes, index)
				}
			//case "RESOURCES_ADDED":
			//	{
			//		// End of epilog: Resource Added event from BDA so release the message
			//		// pop blocked event
			//		var to_add_event Event
			//		to_add_event, epilog_blocked_hpc_events = epilog_blocked_hpc_events[0], epilog_blocked_hpc_events[1:]
			//		// check that the removed resources are the same that are
			//		// allocated by the HPC job
			//		if to_add_event.Data["alloc"] != event.Data["resources"] {
			//			panic("Error in epilog ordering!!!")
			//		}
			//		// Add it to the reply
			//		hpc_reply.Events = append(hpc_reply.Events, to_add_event)
			//		// remove this event from BDA events
			//		to_remove_indexes = append(to_remove_indexes, index)
			//	}
			}
		}
		// Prevent Batsim to receive RESOURCES_REMOVED message
		removeEvents(to_remove_indexes, &bda_reply.Events)

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

		bat_sock.SendBytes(msg, 0)
		fmt.Println("Batsim <= Broker(HPC+BDA):\n", string(msg))
		fmt.Println("------------------------------------------")
	}
}
