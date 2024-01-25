SHELL=/bin/bash

BIN_DIR            := $(shell pwd)/bin
BUF                := $(abspath $(BIN_DIR)/buf)
PROTOC_GEN_GO      := $(abspath $(BIN_DIR)/protoc-gen-go)
PROTOC_GEN_GO_GRPC := $(abspath $(BIN_DIR)/protoc-gen-go-grpc)

$(BIN_DIR):
	mkdir -p bin

buf: $(BIN_DIR) $(BUF)
$(BUF):
	cd ./tools/buf && go build -o ../../bin/buf github.com/bufbuild/buf/cmd/buf

protoc-gen-go: $(BIN_DIR) $(PROTOC_GEN_GO)
$(PROTOC_GEN_GO):
	cd ./tools/protoc-gen-go && go build -o ../../bin/protoc-gen-go github.com/golang/protobuf/protoc-gen-go

protoc-gen-go-grpc: $(BIN_DIR) $(PROTOC_GEN_GO_GRPC)
$(PROTOC_GEN_GO_GRPC):
	cd ./tools/protoc-gen-go-grpc && go build -o ../../bin/protoc-gen-go-grpc google.golang.org/grpc/cmd/protoc-gen-go-grpc

.PHONY: gen
gen: $(BUF) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	$(BUF) generate --path ./proto/worker/v1/worker.proto
