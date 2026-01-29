package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	protocol "purpura.dev.br/study/grpc/protocol"
)

type service struct {
	protocol.UnimplementedProtocolServer
}

func (_ *service) Operation(_ context.Context, request *protocol.Request) (*protocol.Response, error) {
	response := &protocol.Response_builder{
		Message: proto.String("Test"),
	}
	return response.Build(), nil
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	protocol.RegisterProtocolServer(server, &service{})
	err = server.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}
