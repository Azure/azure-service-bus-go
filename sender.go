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
	"fmt"
	"math/rand"
	"time"

	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/azure-amqp-common-go/uuid"
	"github.com/opentracing/opentracing-go"
	"pack.ag/amqp"
)

// sender provides session and link handling for an sending entity path
type (
	sender struct {
		namespace  *Namespace
		connection *amqp.Client
		session    *session
		sender     *amqp.Sender
		entityPath string
		Name       string
		sessionID  *string
	}

	// SendOption provides a way to customize a message on sending
	SendOption func(event *Message) error

	eventer interface {
		Set(key, value string)
		toMsg() (*amqp.Message, error)
	}

	// senderOption provides a way to customize a sender
	senderOption func(*sender) error
)

// newSender creates a new Service Bus message sender given an AMQP client and entity path
func (ns *Namespace) newSender(ctx context.Context, entityPath string, opts ...senderOption) (*sender, error) {
	span, ctx := ns.startSpanFromContext(ctx, "sb.sender.newSender")
	defer span.Finish()

	s := &sender{
		namespace:  ns,
		entityPath: entityPath,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			log.For(ctx).Error(err)
			return nil, err
		}
	}

	err := s.newSessionAndLink(ctx)
	if err != nil {
		log.For(ctx).Error(err)
	}
	return s, err
}

// Recover will attempt to close the current session and link, then rebuild them
func (s *sender) Recover(ctx context.Context) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "sb.sender.Recover")
	defer span.Finish()

	// we expect the sender, session or client is in an error state, ignore errors
	closeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	closeCtx = opentracing.ContextWithSpan(closeCtx, span)
	defer cancel()
	_ = s.sender.Close(closeCtx)
	_ = s.session.Close(closeCtx)
	_ = s.connection.Close()
	return s.newSessionAndLink(ctx)
}

// Close will close the AMQP connection, session and link of the sender
func (s *sender) Close(ctx context.Context) error {
	span, _ := s.startProducerSpanFromContext(ctx, "sb.sender.Close")
	defer span.Finish()

	return s.connection.Close()
}

// Send will send a message to the entity path with options
//
// This will retry sending the message if the server responds with a busy error.
func (s *sender) Send(ctx context.Context, event *Message, opts ...SendOption) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "sb.sender.Send")
	defer span.Finish()

	if event.GroupID == nil {
		event.GroupID = &s.session.SessionID
		next := s.session.getNext()
		event.GroupSequence = &next
	}

	if event.ID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			log.For(ctx).Error(err)
			return err
		}
		event.ID = id.String()
	}

	for _, opt := range opts {
		err := opt(event)
		if err != nil {
			log.For(ctx).Error(err)
			return err
		}
	}

	return s.trySend(ctx, event)
}

func (s *sender) trySend(ctx context.Context, evt eventer) error {
	sp, ctx := s.startProducerSpanFromContext(ctx, "sb.sender.trySend")
	defer sp.Finish()

	err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, evt)
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	msg, err := evt.toMsg()
	if err != nil {
		return err
	}
	sp.SetTag("sb.message-id", msg.Properties.MessageID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// try as long as the context is not dead
			err = s.sender.Send(ctx, msg)
			if err == nil {
				// successful send
				return err
			}

			switch err.(type) {
			case *amqp.Error, *amqp.DetachError:
				log.For(ctx).Debug("amqp error, delaying 4 seconds: " + err.Error())
				skew := time.Duration(rand.Intn(1000)-500) * time.Millisecond
				time.Sleep(4*time.Second + skew)
				err := s.Recover(ctx)
				if err != nil {
					log.For(ctx).Debug("failed to recover connection")
				}
				log.For(ctx).Debug("recovered connection")
			default:
				fmt.Println(err.Error())
				return err
			}
		}
	}
}

func (s *sender) String() string {
	return s.Name
}

func (s *sender) getAddress() string {
	return s.entityPath
}

func (s *sender) getFullIdentifier() string {
	return s.namespace.getEntityAudience(s.getAddress())
}

// newSessionAndLink will replace the existing session and link
func (s *sender) newSessionAndLink(ctx context.Context) error {
	span, ctx := s.startProducerSpanFromContext(ctx, "sb.sender.newSessionAndLink")
	defer span.Finish()

	connection, err := s.namespace.newConnection()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}
	s.connection = connection

	err = s.namespace.negotiateClaim(ctx, connection, s.getAddress())
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	amqpSession, err := connection.NewSession()
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	amqpSender, err := amqpSession.NewSender(
		amqp.LinkTargetAddress(s.getAddress()),
		amqp.LinkSenderSettle(amqp.ModeMixed))
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}

	s.session, err = newSession(amqpSession)
	if err != nil {
		log.For(ctx).Error(err)
		return err
	}
	if s.sessionID != nil {
		s.session.SessionID = *s.sessionID
	}

	s.sender = amqpSender
	return nil
}

// sendWithSession configures the message to send with a specific session and sequence. By default, a sender has a
// default session (uuid.NewV4()) and sequence generator.
func sendWithSession(sessionID string) senderOption {
	return func(event *sender) error {
		event.sessionID = &sessionID
		return nil
	}
}
