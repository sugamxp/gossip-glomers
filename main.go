package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	node     *maelstrom.Node
	mtx      sync.RWMutex
	msg_map  map[int]string
	topology map[string]any
}

const (
	NOT_REPLICATED = "NOT_REPLICATED"
	REPLICATED     = "REPLICATED"
)

func main() {

	n := maelstrom.NewNode()
	server := &Server{node: n, msg_map: map[int]string{}, topology: map[string]any{}}

	n.Handle("echo", server.handleEcho)

	n.Handle("generate", server.handleGenerate)

	n.Handle("broadcast", server.handleBroadCast)

	n.Handle("read", server.handleRead)

	n.Handle("topology", server.handleTopology)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

func (server *Server) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return server.node.Reply(msg, body)
}

func (server *Server) handleGenerate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = uuid.New().String()
	return server.node.Reply(msg, body)
}

func (server *Server) handleBroadCast(msg maelstrom.Message) error {
	server.mtx.Lock()
	defer server.mtx.Unlock()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	incoming_msg := int(body["message"].(float64))

	if _, ok := server.msg_map[incoming_msg]; !ok {
		//do something here
		server.msg_map[incoming_msg] = NOT_REPLICATED

		//TODO - send message to neighbors
		neighbors := server.topology["topology"].(map[string]any)[server.node.ID()].([]interface{})
		log.Println(neighbors)

		for _, nei := range neighbors {
			delete(body, "message_id")
			log.Println("neighbor : " + nei.(string))
			server.node.Send(nei.(string), body)

			//TODO - Challenge 3c
			// body["type"] = "broadcast-to-neighbor"
			// n.RPC(nei.(string), body, func(msg maelstrom.Message) error {

			// })

		}

		server.msg_map[incoming_msg] = REPLICATED
	}

	body["type"] = "broadcast_ok"
	delete(body, "message")
	return server.node.Reply(msg, body)

}

func (server *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"

	keys := make([]any, len(server.msg_map))
	i := 0
	for k := range server.msg_map {
		keys[i] = k
		i++
	}

	body["messages"] = keys
	return server.node.Reply(msg, body)

}

func (server *Server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	server.topology["topology"] = body["topology"]

	log.Println("==> topo")
	log.Println(json.Marshal(server.topology))
	body["type"] = "topology_ok"
	delete(body, "topology")
	return server.node.Reply(msg, body)

}
