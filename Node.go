package main 

import (
	"context" 
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	proto "github.com/bertram-a11y/Consensus_Golfers/gRPC" 
	"google.golang.org/grpc"
)

//hardcoded
var ports = []int{5001, 5002, 5003}

type Node struct {
	proto.UnimplementedNodeServer
	mu       sync.Mutex
	id       int64 
	want     bool
	in_use   bool
	clock    int64         
	reqClock int64         
	released chan struct{} 
	peers    []proto.NodeClient
}

func main() {
	id := flag.Int64("id", 1, "Node ID (1-3)")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("Node %d: ", *id))

	// Create a new Node instance
	node := &Node{id: *id}

	//Server part
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 5000+*id)) 
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// Create a new gRPC server
	server := grpc.NewServer()

	// Register the Node service with the gRPC server
	proto.RegisterNodeServer(server, node) 

	go server.Serve(listener) 
	log.Printf("listening on :%d", 5000+*id)

	//Client part
	for _, port := range ports {
		if port == 5000+int(*id) { 
			continue
		}
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
		if err != nil { 
			log.Fatalf("failed to connect to %d: %v", port, err)
		}
		node.peers = append(node.peers, proto.NewNodeClient(conn)) 
	}

	//Request for the critical section at random time intervals
	for {
		time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
		node.enter() 
	}
}

func (n *Node) enter() {
	n.mu.Lock()
	n.want = true
	n.clock = n.clock + 1
	n.reqClock = n.clock
	n.released = make(chan struct{})
	req := &proto.Request{Content: strconv.FormatInt(n.id, 10), Clock: n.reqClock}
	n.mu.Unlock()
	log.Printf("Requesting critical section with clock %d", req.Clock) 

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := peer.GetNode(context.Background(), req, grpc.WaitForReady(true))
			if err != nil { 
				log.Fatalf("request failed: %v", err)
			}

			n.mu.Lock()
			n.clock = max(n.clock, reply.Clock) + 1 
			log.Printf("Received reply from peer with clock %d ", reply.Clock)
			n.mu.Unlock()
		}() 
	} 
	wg.Wait()

	n.mu.Lock()
	n.want = false
	n.in_use = true
	clock := n.clock
	n.mu.Unlock() 

	log.Printf("Entering critical section with clock %d", clock)
	time.Sleep(500 * time.Millisecond) 
	log.Printf("Leaving critical section with clock %d", clock)

	n.mu.Lock()
	n.in_use = false
	close(n.released)
	n.mu.Unlock()
}


func (n *Node) GetNode(ctx context.Context, req *proto.Request) (*proto.Reply, error) {
	from, _ := strconv.ParseInt(req.Content, 10, 64)
	n.mu.Lock()
	n.clock = max(n.clock, req.Clock) + 1

	weFirst := n.in_use || (n.want && (n.reqClock < req.Clock || (n.reqClock == req.Clock && n.id < from)))
	released := n.released
	if weFirst { 
		log.Printf("Received request from peer %d with clock %d, but we are first (clock %d)", from, req.Clock, n.clock)
	}
	n.mu.Unlock()

	if weFirst {
		<-released
	}

	n.mu.Lock()
	n.clock = n.clock + 1
	clock := n.clock
	n.mu.Unlock()
	log.Printf("Sending reply to peer %d with clock %d", from, clock) 

	return &proto.Reply{Content: strconv.FormatInt(n.id, 10), Clock: clock}, nil
}