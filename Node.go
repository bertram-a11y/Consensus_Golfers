package Node

import (
	proto "gRPC/proto"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type Node struct {
	proto.unimplementedNodeServiceServer

	NodeID string
}

func main() {
	// Create a new gRPC server
	server := grpc.NewServer()

	// Create a new Node instance
	node := &Node{
		NodeID: "node-1",
	}

	// Register the Node service with the gRPC server
	proto.RegisterNodeServiceServer(server, node)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Println("gRPC server listening on :50051 nigga")
	if err := server.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

type LamportClock struct {
	mu   sync.Mutex
	time int64
}

func (c *LamportClock) Update(t int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time = max(c.time, t) + 1
	return c.time
}

func (c *LamportClock) Tick() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time++
	return c.time
}
