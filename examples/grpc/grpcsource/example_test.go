package grpcsource_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/sagarsinghdev/coherent"
	coherentv1 "github.com/sagarsinghdev/coherent/examples/grpc/gen/coherentv1"
	"github.com/sagarsinghdev/coherent/examples/grpc/grpcsource"
	"github.com/sagarsinghdev/coherent/examples/grpc/refserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Example wires a full round trip over an in-process gRPC connection: a reference
// owner server, a gRPC-streaming Source feeding a coherent.Handler, and a local
// MemCache. When the owner publishes an invalidation, the consumer's cached copy
// is evicted within milliseconds.
func Example() {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Owner side: a reference InvalidationService over an in-process listener.
	lis := bufconn.Listen(1 << 20)
	owner := refserver.New(64, 1024)
	gs := grpc.NewServer()
	owner.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	// Consumer side: dial the owner and build a gRPC-streaming Source.
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close() }()

	src, err := grpcsource.New(grpcsource.Options[string]{
		Client:       coherentv1.NewInvalidationServiceClient(conn),
		SubscriberID: "example-consumer",
		KeyDecoder:   func(k string) (string, error) { return k, nil },
		Logger:       quiet,
	})
	if err != nil {
		panic(err)
	}

	cache := coherent.NewMemCache[string, string](coherent.Options[string, string]{})
	handler := coherent.NewHandler[string, string](cache, src, quiet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = handler.Run(ctx) }()

	// Wait until the consumer is connected to the owner.
	waitFor(func() bool { return owner.Active() == 1 })

	// Populate a value locally. Re-set until it sticks past the one-time
	// clear-on-connect the Source emits, then read it back from the local cache.
	waitFor(func() bool {
		cache.Set("user:42", "Ada")
		time.Sleep(5 * time.Millisecond)
		_, ok := cache.Get("user:42")
		return ok
	})
	fmt.Println(get(cache, "user:42"))

	// The owner mutates user:42 and broadcasts the invalidation.
	owner.Publish("user:42", "updated")

	// The consumer evicts its now-stale copy asynchronously.
	waitFor(func() bool { _, ok := cache.Get("user:42"); return !ok })
	fmt.Println(get(cache, "user:42"))

	// Output:
	// Ada
	// <miss>
}

func get(c *coherent.MemCache[string, string], k string) string {
	if v, ok := c.Get(k); ok {
		return v
	}
	return "<miss>"
}

// waitFor polls cond until it is true or a generous deadline elapses.
func waitFor(cond func() bool) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	panic("condition not met within timeout")
}
