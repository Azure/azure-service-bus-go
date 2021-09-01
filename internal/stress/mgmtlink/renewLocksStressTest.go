package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/joho/godotenv"
)

const QUEUE_PREFETCH = 1000
const RENEWALS_PER_MESSAGE = 2

func main() {
	ctx := context.Background()

	godotenv.Load(".env")
	cs := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	queueName := "samples"

	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(cs))

	if err != nil {
		log.Fatalf("Failed to create namespace client: %s", err.Error())
	}

	sender, err := ns.NewSender(ctx, queueName)

	if err != nil {
		log.Fatalf("Failed to create sender: %s", err.Error())
	}

	ch := make(chan bool, 100)

	for i := 0; i < QUEUE_PREFETCH; i++ {
		ch <- true
		go func() {
			defer func() { <-ch }()
			if err = sender.Send(ctx, &servicebus.Message{Data: []byte(fmt.Sprintf("hello world %d", i))}); err != nil {
				log.Fatalf("Failed to send message: %s", err.Error())
			}
		}()
	}

	queue, err := ns.NewQueue(queueName, servicebus.QueueWithPrefetchCount(QUEUE_PREFETCH))

	if err != nil {
		log.Fatalf("Failed to create receiver: %s", err.Error())
	}

	renewals := int32(0)
	outstandingRenewals := int32(0)
	failedRenewals := int32(0)
	lastRenewalWasFailure := int32(0)

	wg := &sync.WaitGroup{}

	go func() {
		ticker := time.NewTicker(time.Second * 5)

		for {
			select {
			case <-ticker.C:
				log.Printf("Messages: [total: %d, outstanding: %d, failed: %d]", atomic.LoadInt32(&renewals), atomic.LoadInt32(&outstandingRenewals), atomic.LoadInt32(&failedRenewals))
			}
		}
	}()

	err = queue.Receive(ctx, servicebus.HandlerFunc(func(c context.Context, m *servicebus.Message) error {
		wg.Add(1)

		go func() {
			worked := false

			for i := 0; i < RENEWALS_PER_MESSAGE; i++ {
				atomic.AddInt32(&outstandingRenewals, 1)
				if err := queue.RenewLocks(ctx, m); err != nil {
					worked = false
					atomic.AddInt32(&failedRenewals, 1)
					log.Printf("ERROR renewing: %+v", err)
				} else {
					worked = true
				}

				atomic.AddInt32(&outstandingRenewals, -1)
				atomic.AddInt32(&renewals, 1)
			}

			m.Complete(ctx)

			if !worked {
				log.Printf("Last renewal was a failure here.")
				atomic.AddInt32(&lastRenewalWasFailure, 1)
			}

			wg.Done()
		}()

		return nil
	}))

	log.Printf("Last Renewal was failure: %d", lastRenewalWasFailure)

	if err != nil {
		log.Fatalf("Failed to receive messages")
	}

	wg.Wait()
}
