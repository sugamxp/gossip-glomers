package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	messages := []any{}
	msg_map := map[any]any{}
	topology := map[string]any{}
	mtx := sync.RWMutex{}

	const (
		NOT_REPLICATED = "NOT_REPLICATED"
		REPLICATED     = "REPLICATED"
	)

	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = uuid.New().String()
		return n.Reply(msg, body)

	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		mtx.Lock()
		defer mtx.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		incoming_msg := body["message"]
		
		messages = append(messages, incoming_msg)
		
		if _, ok := msg_map[incoming_msg]; !ok {
			//do something here
			msg_map[incoming_msg] = NOT_REPLICATED

			//TODO - send message to neighbors
			neighbors := topology["topology"].(map[string]any)[n.ID()].([]interface{})
			log.Println(neighbors)

			for _, nei := range neighbors {
				delete(body, "message_id")
				log.Println("neighbor : " + nei.(string))
				n.Send(nei.(string), body)
			}

			msg_map[incoming_msg] = REPLICATED
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)

	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"

		keys := make([]any, len(msg_map))
		i := 0
		for k := range msg_map {
			keys[i] = k
			i++
		}

		// body["messages"] = messages
		body["messages"] = keys

		return n.Reply(msg, body)

	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology["topology"] = body["topology"]

		log.Println("==> topo")
		log.Println(json.Marshal(topology))
		body["type"] = "topology_ok"
		delete(body, "topology")
		return n.Reply(msg, body)

	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
