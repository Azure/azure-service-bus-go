package servicebus

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"encoding/xml"
	"sync"

	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/go-autorest/autorest/date"
)

type (
	// Topic in contrast to queues, in which each message is processed by a single consumer, topics and subscriptions
	// provide a one-to-many form of communication, in a publish/subscribe pattern. Useful for scaling to very large
	// numbers of recipients, each published message is made available to each subscription registered with the topic.
	// Messages are sent to a topic and delivered to one or more associated subscriptions, depending on filter rules
	// that can be set on a per-subscription basis. The subscriptions can use additional filters to restrict the
	// messages that they want to receive. Messages are sent to a topic in the same way they are sent to a queue,
	// but messages are not received from the topic directly. Instead, they are received from subscriptions. A topic
	// subscription resembles a virtual queue that receives copies of the messages that are sent to the topic.
	// Messages are received from a subscription identically to the way they are received from a queue.
	Topic struct {
		*entity
		sender   *sender
		senderMu sync.Mutex
	}

	// TopicDescription is the content type for Topic management requests
	TopicDescription struct {
		XMLName xml.Name `xml:"TopicDescription"`
		BaseEntityDescription
		DefaultMessageTimeToLive            *string       `xml:"DefaultMessageTimeToLive,omitempty"`            // DefaultMessageTimeToLive - ISO 8601 default message time span to live value. This is the duration after which the message expires, starting from when the message is sent to Service Bus. This is the default value used when TimeToLive is not set on a message itself.
		MaxSizeInMegabytes                  *int32        `xml:"MaxSizeInMegabytes,omitempty"`                  // MaxSizeInMegabytes - The maximum size of the queue in megabytes, which is the size of memory allocated for the queue. Default is 1024.
		RequiresDuplicateDetection          *bool         `xml:"RequiresDuplicateDetection,omitempty"`          // RequiresDuplicateDetection - A value indicating if this queue requires duplicate detection.
		DuplicateDetectionHistoryTimeWindow *string       `xml:"DuplicateDetectionHistoryTimeWindow,omitempty"` // DuplicateDetectionHistoryTimeWindow - ISO 8601 timeSpan structure that defines the duration of the duplicate detection history. The default value is 10 minutes.
		EnableBatchedOperations             *bool         `xml:"EnableBatchedOperations,omitempty"`             // EnableBatchedOperations - Value that indicates whether server-side batched operations are enabled.
		SizeInBytes                         *int64        `xml:"SizeInBytes,omitempty"`                         // SizeInBytes - The size of the queue, in bytes.
		FilteringMessagesBeforePublishing   *bool         `xml:"FilteringMessagesBeforePublishing,omitempty"`
		IsAnonymousAccessible               *bool         `xml:"IsAnonymousAccessible,omitempty"`
		Status                              *EntityStatus `xml:"Status,omitempty"`
		CreatedAt                           *date.Time    `xml:"CreatedAt,omitempty"`
		UpdatedAt                           *date.Time    `xml:"UpdatedAt,omitempty"`
		SupportOrdering                     *bool         `xml:"SupportOrdering,omitempty"`
		AutoDeleteOnIdle                    *string       `xml:"AutoDeleteOnIdle,omitempty"`
		EnablePartitioning                  *bool         `xml:"EnablePartitioning,omitempty"`
		EnableSubscriptionPartitioning      *bool         `xml:"EnableSubscriptionPartitioning,omitempty"`
		EnableExpress                       *bool         `xml:"EnableExpress,omitempty"`
		CountDetails                        *CountDetails `xml:"CountDetails,omitempty"`
	}

	// TopicOption represents named options for assisting Topic message handling
	TopicOption func(*Topic) error
)

// NewTopic creates a new Topic Sender
func (ns *Namespace) NewTopic(name string, opts ...TopicOption) (*Topic, error) {
	topic := &Topic{
		entity: &entity{
			namespace: ns,
			Name:      name,
		},
	}

	for i := range opts {
		if err := opts[i](topic); err != nil {
			return nil, err
		}
	}

	return topic, nil
}

// Send sends messages to the Topic
func (t *Topic) Send(ctx context.Context, event *Message, opts ...SendOption) error {
	span, ctx := t.startSpanFromContext(ctx, "sb.Topic.Send")
	defer span.Finish()

	err := t.ensureSender(ctx)
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}
	return t.sender.Send(ctx, event, opts...)
}

// Close the underlying connection to Service Bus
func (t *Topic) Close(ctx context.Context) error {
	span, ctx := t.startSpanFromContext(ctx, "sb.Topic.Close")
	defer span.Finish()

	if t.sender != nil {
		return t.sender.Close(ctx)
	}

	return nil
}

func (t *Topic) ensureSender(ctx context.Context) error {
	span, ctx := t.startSpanFromContext(ctx, "sb.Topic.ensureSender")
	defer span.Finish()

	t.senderMu.Lock()
	defer t.senderMu.Unlock()

	if t.sender == nil {
		s, err := t.namespace.newSender(ctx, t.Name)
		if err != nil {
			log.For(ctx).Error(err)
			return err
		}
		t.sender = s
	}
	return nil
}
