# Raft Go SideCar Library

## Overview

The Raft Go SideCar Library is a Go implementation of the Raft consensus algorithm, designed to provide a reliable way to achieve distributed consensus in a network of nodes as a side-car to your application. It is built with a focus on simplicity, performance, and scalability. The library uses Go channels for communication instead of locks, ensuring runtime consistency without the complexity of traditional locking mechanisms.

## Key Features

### v1

- **Channel-Based Communication**: Utilizes Go channels for message passing and state synchronization, reducing complexity and increasing maintainability.
- **Simplified Runtime**: Keeps runtime complexity low with a structured approach to state changes and message handling.
- **Ease of Integration as SideCar**: Designed for straightforward integration into distributed applications as a SideCar, with minimal overhead for managing Raft logic.
- **Robust Consensus Algorithm**: Implements the Raft consensus algorithm, providing a robust mechanism for ensuring consistency across distributed systems.

### v2

- **Joint Consensus Support**: Implementing the Joint Consensus mechanism, this library can now handle more complex scenarios involving adding or removing multiple nodes from the cluster safely.
- **Dynamic Cluster Reconfiguration**: Easily add or remove nodes from the Raft cluster on the fly, without interrupting ongoing operations.

## Usage

To use the Raft Go SideCar Library in your application, follow these steps:

1. **Initialization**: Create a new Raft server instance by providing node ID, address, peer information, storage, and log interface implementations.
2. **Running the Server**: Start the server by calling the `Run` method with a context and a channel for commit entries.
3. **Submitting Commands**: To submit commands to the Raft cluster, use the `Submit` method provided by the server instance. This method can be called with any command that needs to be replicated across the cluster.
4. **Handling State Changes**: The library handles state changes internally, ensuring that all nodes in the cluster stay synchronized.

### Example

Here's an example of how to use the library in a distributed application:

```shell
$ go run cmd/raft/main.go -id 0 &
$ go run cmd/raft/main.go -id 1 &
$ grpc_cli call localhost:50000 proto.PeerService.ChangeConfiguration \
  "newConfig: {peers: [{id: 0, address: 'localhost:50000'}, {id: 1, address: 'localhost:50001'}]}"
$ go run cmd/raft/main.go -id 2 &
$ grpc_cli call localhost:50000 proto.PeerService.ChangeConfiguration \
  "newConfig: {peers: [{id: 0, address: 'localhost:50000'}, {id: 1, address: 'localhost:50001'}, {id: 2, address: 'localhost:50002'}]}"
$ grpc_cli call localhost:50000 proto.PeerService.Submit "command: 'hello'"
```

```go
ctx, cancel := context.WithCancel(context.Background())

// Create storage and log implementations
storage := NewFileStorage("path/to/storage")
if err := storage.Open(); err != nil {
	// handle error
}
defer storage.Close()
log := NewLevelDBLogStorage("path/to/logdb")
defer log.Close()

// Initialize the Raft server
server := raft.NewServer(1, "127.0.0.1:5000", map[int32]*raft.Peer{}, storage, log)

commitCh := make(chan raft.Entry)

go func() {
    for {
        select {
        case commit := <-commitCh:
            log.Printf("Command received: %s", <-commitCh)
        case <-ctx.Done():
            return
        }
    }
}()

// Start the server in a separate goroutine
go server.Run(ctx, commitCh)
```

## Test Cases

The library includes an extensive suite of end-to-end test cases demonstrating various aspects of Raft's functionality, including leader election, log replication, and node failure recovery. These tests can serve as examples for implementing and testing your own distributed applications using the Raft Go Channel Library.

## Project Structure

The library is structured as follows:

```
.
├── Makefile
├── README.md
├── bin
│   ├── buf
│   ├── protoc-gen-go
│   └── protoc-gen-go-grpc
├── buf.gen.yaml
├── cmd
│   └── raft
│       └── main.go
├── file.go
├── file_test.go
├── go.mod
├── go.sum
├── leveldb.go
├── leveldb_test.go
├── proto
│   └── peer
│       └── v1
│           ├── peer.pb.go
│           ├── peer.proto
│           └── peer_grpc.pb.go
├── raft.go
├── raft_test.go
└── tools
    └── ...
```

## License

This library is open-source and licensed under the MIT License.
