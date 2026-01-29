// Copyright (c) 2025 Pedro Lamarão. All rights reserved.

package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	protocol "purpura.dev.br/study/grpc/protocol"
)

func main() {
	connection, err := grpc.NewClient("127.0.0.1:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	requestor := protocol.NewProtocolClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	request := protocol.Request_builder{
		Message: proto.String(""),
	}
	response, err := requestor.Operation(ctx, request.Build())
	if err != nil {
		log.Fatal(err)
	}

	log.Print(response.GetMessage())
}
