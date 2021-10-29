package servicebus_test

import (
	"context"
	"fmt"
	"os"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
)

func Example_topicSendAndReceive() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	if connStr == "" {
		fmt.Println("FATAL: expected environment variable SERVICEBUS_CONNECTION_STRING not set")
		return
	}

	// Create a client to communicate with a Service Bus Namespace.
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create a client to communicate with the topic. (The topic must have already been created, see `TopicManager`)
	topic, err := ns.NewTopic("topic-name")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Send a message to the topic
	err = topic.Send(ctx, servicebus.NewMessageFromString("Hello, World!!!"))
	if err != nil {
		fmt.Println("FATAL: ", err)
		return
	}

	// Create a client to communicate with a topic subscription.
	subscription, err := topic.NewSubscription("subscription-name")

	if err != nil {
		fmt.Println("FATAL: ", err)
		return
	}

	// Listen the topic subscription to receive the message
	err = subscription.ReceiveOne(
		ctx,
		servicebus.HandlerFunc(func(ctx context.Context, message *servicebus.Message) error {
			fmt.Println(string(message.Data))
			return message.Complete(ctx)
		}))
	if err != nil {
		fmt.Println("FATAL: ", err)
		return
	}

	// Output: Hello, World!!!
}
