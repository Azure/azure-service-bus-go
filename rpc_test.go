package servicebus

import (
	"context"
	"testing"

	"github.com/Azure/azure-amqp-common-go/v3/rpc"
	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
)

func TestRPCLinkCaching(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("WithClose", func(t *testing.T) {
		fake := createFakeRPCClient()

		// validate our internal cache properly tracked the link
		// now getting the link again should get us the cached link.
		addCachedLink(ctx, t, fake)

		// close the client itself
		require.NoError(t, fake.rpcClient.Close())

		require.True(t, fake.amqpClientClosed, "AMQP client was closed")
		require.Empty(t, fake.rpcClient.linkCache, "link cache is cleared when PRC client is closed")
	})

	t.Run("WithRecover", func(t *testing.T) {
		fake := createFakeRPCClient()

		// validate our internal cache properly tracked the link
		// now getting the link again should get us the cached link.
		addCachedLink(ctx, t, fake)

		// Recover the client instead.
		require.NoError(t, fake.rpcClient.Recover(ctx))

		require.True(t, fake.amqpClientClosed, "AMQP client was closed")
		require.Empty(t, fake.rpcClient.linkCache, "link cache is cleared when PRC client recovers")

		// sanity check: just make sure nothing breaks if we close afterwards
		require.NoError(t, fake.rpcClient.Close())
	})
}

func addCachedLink(ctx context.Context, t *testing.T, fake *fakeRPCClient) {
	createdLink, err := fake.rpcClient.getCachedLink(ctx, "an address")

	require.NoError(t, err)
	require.EqualValues(t, 1, len(fake.createdLinks))
	require.NotNil(t, createdLink)

	linkFromInternalCache, exists := fake.rpcClient.linkCache["an address"]
	require.True(t, exists)
	require.Same(t, linkFromInternalCache, createdLink)

	sameOldLink, err := fake.rpcClient.getCachedLink(ctx, "an address")
	require.NoError(t, err)

	require.Same(t, sameOldLink, createdLink, "Same link instance should always be returned")
}

type fakeRPCClient struct {
	*rpcClient
	newAMQPClientCreated bool
	amqpClientClosed     bool
	createdLinks         []*rpc.Link
}

func createFakeRPCClient() *fakeRPCClient {
	fake := &fakeRPCClient{}

	fake.rpcClient = &rpcClient{
		ec:        &fakeEntityConnector{},
		linkCache: map[string]*rpc.Link{},

		newAMQPClient: func(ctx context.Context, ec entityConnector) (*amqp.Client, func() <-chan struct{}, error) {
			fake.newAMQPClientCreated = true
			ch := make(chan struct{})
			close(ch) // NOTE, it's just a little lie (ie - we'll just pretend cancellation of creds refresh worked INSTANTLY)

			return &amqp.Client{}, func() <-chan struct{} {
				return ch
			}, nil
		},
		newRPCLink: func(conn *amqp.Client, address string, opts ...rpc.LinkOption) (*rpc.Link, error) {
			fake.createdLinks = append(fake.createdLinks, &rpc.Link{})
			return fake.createdLinks[len(fake.createdLinks)-1], nil
		},
		closeAMQPClient: func() error {
			fake.amqpClientClosed = true
			return nil
		},
	}

	return fake
}

type fakeEntityConnector struct{}

func (fc *fakeEntityConnector) ManagementPath() string {
	return "ThisIsTheManagementPath"
}

func (fc *fakeEntityConnector) Namespace() *Namespace {
	return &Namespace{
		amqpDial: func(addr string, opts ...amqp.ConnOption) (*amqp.Client, error) {
			return &amqp.Client{}, nil
		},
	}
}

func (fc *fakeEntityConnector) getEntity() *entity {
	return nil
}
