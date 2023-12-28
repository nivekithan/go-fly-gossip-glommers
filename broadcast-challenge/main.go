package main

import (
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type clientBroadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	FromId  string `json:"fromId"`
}

func main() {
	node := maelstrom.NewNode()

	node_msg := []int{}
	var topology map[string][]string

	node.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMessage struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
		}

		var body topologyMessage

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology

		log.Println(topology)

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

		for _, clientId := range node.NodeIDs() {
			if clientId == node.ID() {
				continue
			}

			go broadcastToOtherNodes(node, body.Message, clientId)
		}

		return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	node.Handle("read", func(msg maelstrom.Message) error {

		return node.Reply(msg, map[string]any{"type": "read_ok", "messages": node_msg})
	})

	node.Handle("client_broadcast_message", func(msg maelstrom.Message) error {
		var body clientBroadcastMessage

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		node_msg = append(node_msg, body.Message)

		return node.Reply(msg, map[string]any{"type": "client_broadcast_message_ok"})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
func broadcastToOtherNodes(node *maelstrom.Node, message int, clientId string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	resChan := make(chan bool)
	if err := node.RPC(
		clientId,
		clientBroadcastMessage{
			Type:    "client_broadcast_message",
			Message: message,
			FromId:  node.ID(),
		},
		func(msg maelstrom.Message) error {
			resChan <- true
			return nil
		},
	); err != nil {
		panic(err)
	}

	select {
	case <-ticker.C:
		broadcastToOtherNodes(node, message, clientId)

	case <-resChan:
		return
	}
}
