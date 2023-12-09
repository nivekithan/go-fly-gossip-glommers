package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	node_msg := []int{}

	node.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response_body := make(map[string]string)

		response_body["type"] = "topology_ok"

		return node.Reply(msg, response_body)
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		type boardcastMessage struct {
			Type    string `json:"type"`
			Message int    `json:"message"`
		}

		var body boardcastMessage

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		node_msg = append(node_msg, body.Message)

		return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {

		return node.Reply(msg, map[string]any{"type": "read_ok", "messages": node_msg})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
