# Raft Go Channel Library

## Overview

The Raft Go Channel Library is a Go implementation of the Raft consensus algorithm, designed to provide a reliable way to achieve distributed consensus in a network of nodes. It is built with a focus on simplicity, performance, and scalability. The library uses Go channels for communication instead of locks, ensuring runtime consistency without the complexity of traditional locking mechanisms.

## Key Features

- **Channel-Based Communication**: Utilizes Go channels for message passing and state synchronization, reducing complexity and increasing maintainability.
- **Simplified Runtime**: Keeps runtime complexity low with a structured approach to state changes and message handling.
- **Ease of Integration**: Designed for straightforward integration into distributed applications, with minimal overhead for managing Raft logic.
- **Robust Consensus Algorithm**: Implements the Raft consensus algorithm, providing a robust mechanism for ensuring consistency across distributed systems.

## Usage

To use the Raft Go Channel Library in your application, follow these steps:

1. **Initialization**: Create a new Raft server instance by providing node ID, address, peer information, storage, and log interface implementations.
2. **Running the Server**: Start the server by calling the `Run` method with a context and a channel for commit entries.
3. **Submitting Commands**: To submit commands to the Raft cluster, use the `Submit` method provided by the server instance. This method can be called with any command that needs to be replicated across the cluster.
4. **Handling State Changes**: The library handles state changes internally, ensuring that all nodes in the cluster stay synchronized.

### Example

Here's an example of how to use the library in a distributed application:

```go
// Create storage and log implementations
storage := NewFileStorage("path/to/storage")
log := NewLevelDBLogStorage("path/to/logdb")

// Define peers in the cluster
peers := map[int32]*Peer{
    1: NewPeer(1, "127.0.0.1:5001"),
    2: NewPeer(2, "127.0.0.1:5002"),
    // Add other peers as needed
}

// Initialize the Raft server
server := raft.NewServer(1, "127.0.0.1:5000", peers, storage, log)

commitCh := make(chan raft.Entry)

go func() {
    for {
        log.Printf("Command received: %s", <-commitCh)
    }
}()

// Start the server in a separate goroutine
go server.Run(context.Background(), commitCh)

// Submit a command to the cluster
cmd := "example_command"
if res, err := server.Submit(context.Background(), &peerv1.SubmitRequest{Command: cmd}); err != nil {
    log.Printf("Error submitting command: %v", err)
} else if !res.Success {
    log.Printf("Command submission failed")
}
```

## Test Cases

The library includes an extensive suite of end-to-end test cases demonstrating various aspects of Raft's functionality, including leader election, log replication, and node failure recovery. These tests can serve as examples for implementing and testing your own distributed applications using the Raft Go Channel Library.

## Project Structure

The library is structured as follows:

```
.
├── cmd
│   └── raft
│       └── main.go
├── pkg
│   ├── log
│   │   ├── leveldb.go
│   │   └── leveldb_test.go
│   ├── raft
│   │   ├── raft.go
│   │   └── utils.go
│   └── storage
│       └── file.go
├── proto
│   └── peer
│       └── v1
│           ├── peer.pb.go
│           ├── peer.proto
│           └── peer_grpc.pb.go
├── test
│   └── e2e
│       └── raft_test.go
└── tools
    └── ...
```

## License

This library is open-source and licensed under the MIT License.
