//go:build ignore

// This synthetic owner answers the service's actual metadata RPC contract and
// publishes its lifecycle event. It is a test fixture, never a service endpoint.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/alkem-io/collaboration-service/internal/adapter/inbound/lifecycle"
	metadata "github.com/alkem-io/collaboration-service/internal/adapter/outbound/metadatastore/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	if len(os.Args) != 4 {
		panic("require broker URL, metadata queue, lifecycle queue")
	}
	conn, err := amqp.Dial(os.Args[1])
	check(err)
	defer conn.Close()
	ch, err := conn.Channel()
	check(err)
	defer ch.Close()
	_, err = ch.QueueDeclare(os.Args[2], true, false, false, false, nil)
	check(err)
	deliveries, err := ch.Consume(os.Args[2], "", true, false, false, false, nil)
	check(err)
	var deleted atomic.Bool
	go func() {
		for msg := range deliveries {
			var req struct {
				Pattern string             `json:"pattern"`
				Data    metadata.FetchData `json:"data"`
				ID      string             `json:"id"`
			}
			check(json.Unmarshal(msg.Body, &req))
			if msg.ReplyTo == "" {
				continue
			}
			var response any
			switch req.Pattern {
			case metadata.PatternFetch:
				response = metadata.FetchReply{Found: req.Data.ID == "image-lifecycle" && !deleted.Load(), ContentType: "memo", Migrated: true}
			case metadata.PatternSave:
				response = metadata.SaveReply{Success: true}
			default:
				panic("unexpected RPC pattern: " + req.Pattern)
			}
			body, err := json.Marshal(map[string]any{"response": response, "id": req.ID, "isDisposed": true})
			check(err)
			check(ch.PublishWithContext(context.Background(), "", msg.ReplyTo, false, false, amqp.Publishing{CorrelationId: msg.CorrelationId, ContentType: "application/json", Body: body}))
		}
	}()
	fmt.Println("READY")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Text() != "delete" {
			panic("unexpected fixture command")
		}
		pub, err := conn.Channel()
		check(err)
		check(pub.Confirm(false))
		confirms := pub.NotifyPublish(make(chan amqp.Confirmation, 1))
		body, err := json.Marshal(map[string]any{"pattern": lifecycle.PatternDocumentDeleted, "data": lifecycle.DeletedEvent{ID: "image-lifecycle"}, "id": "synthetic-delete"})
		check(err)
		check(pub.PublishWithContext(context.Background(), "", os.Args[3], true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: body}))
		select {
		case confirmation := <-confirms:
			if !confirmation.Ack {
				panic("delete not confirmed")
			}
		case <-time.After(10 * time.Second):
			panic("delete confirmation timed out")
		}
		deleted.Store(true)
		check(pub.Close())
		fmt.Println("DELETED")
	}
	check(scanner.Err())
}
