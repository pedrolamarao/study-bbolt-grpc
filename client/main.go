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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	switch os.Args[1] {
	case "bucket":
		requestor := protocol.NewBucketClient(connection)
		switch os.Args[2] {
		case "create":
			request := protocol.CreateBucketRequest_builder{
				Bucket: []byte(os.Args[3]),
			}
			_, err = requestor.CreateBucket(ctx, request.Build())
		case "destroy":
			request := protocol.DestroyBucketRequest_builder{
				Bucket: []byte(os.Args[3]),
			}
			_, err = requestor.DestroyBucket(ctx, request.Build())
		}
		break
	case "value":
		requestor := protocol.NewValueClient(connection)
		switch os.Args[2] {
		case "get":
			request := protocol.GetValueRequest_builder{
				Bucket: []byte(os.Args[3]),
				Key:    []byte(os.Args[4]),
			}
			var response *protocol.GetValueResponse
			response, err = requestor.GetValue(ctx, request.Build())
			if response != nil {
				log.Print(string(response.GetValue()))
			}
			break
		case "set":
			request := protocol.SetValueRequest_builder{
				Bucket: []byte(os.Args[3]),
				Key:    []byte(os.Args[4]),
				Value:  []byte(os.Args[5]),
			}
			_, err = requestor.SetValue(ctx, request.Build())
			break
		case "clear":
			request := protocol.DeleteValueRequest_builder{
				Bucket: []byte(os.Args[3]),
				Key:    []byte(os.Args[4]),
			}
			_, err = requestor.DeleteValue(ctx, request.Build())
			break
		}
		break
	}
	if err != nil {
		log.Fatal(err)
	}
}
