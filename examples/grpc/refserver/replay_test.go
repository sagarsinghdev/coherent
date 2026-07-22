package refserver_test

import (
	"context"
	"net"
	"testing"
	"time"

	coherentv1 "github.com/sagarsinghdev/coherent/examples/grpc/gen/coherentv1"
	"github.com/sagarsinghdev/coherent/examples/grpc/refserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// dialOwner starts owner over an in-process listener and returns a connected
// client plus a cleanup func.
func dialOwner(t *testing.T, owner *refserver.Server) (coherentv1.InvalidationServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	owner.Register(gs)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return coherentv1.NewInvalidationServiceClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
	}
}

// recvWithin returns the next event, failing if none arrives within d.
func recvWithin(t *testing.T, stream coherentv1.InvalidationService_SubscribeClient, d time.Duration) *coherentv1.InvalidationEvent {
	t.Helper()
	type res struct {
		ev  *coherentv1.InvalidationEvent
		err error
	}
	ch := make(chan res, 1)
	go func() {
		ev, err := stream.Recv()
		ch <- res{ev, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Recv: %v", r.err)
		}
		return r.ev
	case <-time.After(d):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

// TestReplayThenLive proves the full Subscribe flow: a consumer that reconnects
// with a watermark first receives exactly the events it missed (replayed from the
// log in order), then transitions to live delivery on the same stream.
func TestReplayThenLive(t *testing.T) {
	owner := refserver.New(64, 1024)
	client, cleanup := dialOwner(t, owner)
	defer cleanup()

	// Two events happen before/while the consumer is away.
	tA := owner.Publish("a", "updated")
	owner.Publish("b", "updated")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Reconnect resuming after "a": must replay only "b".
	stream, err := client.Subscribe(ctx, &coherentv1.SubscribeRequest{
		SubscriberId:  "consumer-1",
		ResumeAfterMs: tA,
	})
	if err != nil {
		t.Fatal(err)
	}

	if ev := recvWithin(t, stream, 2*time.Second); ev.GetKey() != "b" || ev.GetIsCacheClear() {
		t.Fatalf("replay = %+v; want key=b, no clear", ev)
	}

	// Now a live mutation must flow on the same stream.
	owner.Publish("c", "updated")
	if ev := recvWithin(t, stream, 2*time.Second); ev.GetKey() != "c" {
		t.Fatalf("live = %+v; want key=c", ev)
	}
}

// TestRetentionGapSendsClear proves that when a consumer's watermark predates the
// retained history (older events were dropped), the server sends exactly one
// cache-clear instead of an incomplete replay.
func TestRetentionGapSendsClear(t *testing.T) {
	owner := refserver.New(64, 2) // retain only the newest 2 events
	client, cleanup := dialOwner(t, owner)
	defer cleanup()

	tA := owner.Publish("a", "updated") // will be evicted
	owner.Publish("b", "updated")
	owner.Publish("c", "updated") // evicts "a"; retained: b, c

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Subscribe(ctx, &coherentv1.SubscribeRequest{
		SubscriberId:  "consumer-2",
		ResumeAfterMs: tA, // predates oldest retained (b) -> gap
	})
	if err != nil {
		t.Fatal(err)
	}

	ev := recvWithin(t, stream, 2*time.Second)
	if !ev.GetIsCacheClear() {
		t.Fatalf("expected a single cache-clear on retention gap; got %+v", ev)
	}
}
