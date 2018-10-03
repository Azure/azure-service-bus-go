package servicebus

import (
	"context"
	"fmt"
	"github.com/Azure/azure-amqp-common-go/log"
	"github.com/Azure/azure-amqp-common-go/rpc"
	"github.com/Azure/azure-amqp-common-go/uuid"
	otlogger "github.com/opentracing/opentracing-go/log"
	"time"

	"pack.ag/amqp"
)

const (
	serviceBuslockRenewalOperationName = "com.microsoft:renew-lock"
)

//RenewLocks renews the locks on messages provided
func (e *entity) RenewLocks(ctx context.Context, messages []*Message) error {
	span, ctx := e.startSpanFromContext(ctx, "sb.entity.renewLocks")
	defer span.Finish()

	lockTokens := make([]*uuid.UUID, 0, len(messages))
	for _, m := range messages {
		if m.LockToken == nil {
			log.For(ctx).Error(fmt.Errorf("failed: message has nil lock token, cannot renew lock"), otlogger.Object("messageId", m))
			continue
		}

		lockTokens = append(lockTokens, m.LockToken)
	}

	if len(lockTokens) < 1 {
		log.For(ctx).Info("no lock tokens present to renew")
		return nil
	}

	e.renewMessageLockMutex.Lock()
	defer e.renewMessageLockMutex.Unlock()

	messageID, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("error creating messageID: %+v", err)
	}

	replyToAddress, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("error creating replyToAddress: %+v", err)
	}

	renewRequestMsg := &amqp.Message{
		ApplicationProperties: map[string]interface{}{
			"operation": serviceBuslockRenewalOperationName,
		},
		Properties: &amqp.MessageProperties{
			MessageID: messageID,
			ReplyTo:   replyToAddress.String(),
		},
		Value: map[string]interface{}{
			"lock-tokens": lockTokens,
		},
	}

	conn, err := e.namespace.newConnection()
	if err != nil {
		return err
	}

	rpcLink, err := rpc.NewLink(conn, e.namespace.getEntityManagementPath(e.Name))
	if err != nil {
		return err
	}

	response, err := rpcLink.RetryableRPC(ctx, 3, 1*time.Second, renewRequestMsg)
	if err != nil {
		return err
	}

	if response.Code != 200 {
		return fmt.Errorf("error renewing locks: %v", response.Description)
	}

	return nil
}
