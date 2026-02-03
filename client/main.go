// Copyright (c) 2025 Pedro Lamarão. All rights reserved.

package main

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"purpura.dev.br/study/bbolt-grpc/protocol"
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

	switch os.Args[1] {
	case "get":
		request := protocol.GetRequest_builder{
			Bucket: []byte(os.Args[2]),
			Key:    []byte(os.Args[3]),
		}
		response, err := requestor.Get(ctx, request.Build())
		if err != nil {
			log.Fatal(err)
		}
		log.Print(string(response.GetValue()))
		break
	case "set":
		request := protocol.SetRequest_builder{
			Bucket: []byte(os.Args[2]),
			Key:    []byte(os.Args[3]),
			Value:  []byte(os.Args[4]),
		}
		_, err := requestor.Set(ctx, request.Build())
		if err != nil {
			log.Fatal(err)
		}
		break
	case "clear":
		request := protocol.ClearRequest_builder{
			Bucket: []byte(os.Args[2]),
			Key:    []byte(os.Args[3]),
		}
		_, err := requestor.Clear(ctx, request.Build())
		if err != nil {
			log.Fatal(err)
		}
		break
	}
}
