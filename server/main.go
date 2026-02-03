package main

import (
	"context"
	"log"
	"net"

	"go.etcd.io/bbolt"

	"google.golang.org/grpc"
	"purpura.dev.br/study/bbolt-grpc/protocol"
)

type service struct {
	protocol.UnimplementedProtocolServer

	db *bbolt.DB
}

func (srv *service) Get(_ context.Context, request *protocol.GetRequest) (*protocol.GetResponse, error) {
	var value []byte
	err := srv.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(request.GetBucket())
		if bucket == nil {
			return nil
		}
		value = bucket.Get(request.GetKey())
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.GetResponse_builder{
		Value: value,
	}
	return response.Build(), nil
}

func (srv *service) Set(_ context.Context, request *protocol.SetRequest) (*protocol.SetResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(request.GetBucket())
		if err != nil {
			return err
		}
		err = bucket.Put(request.GetKey(), request.GetValue())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.SetResponse_builder{}
	return response.Build(), nil
}

func (srv *service) Clear(_ context.Context, request *protocol.ClearRequest) (*protocol.ClearResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(request.GetBucket())
		if err != nil {
			return err
		}
		err = bucket.Delete(request.GetKey())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.ClearResponse_builder{}
	return response.Build(), nil
}

func main() {
	db, err := bbolt.Open("service.db", 0600, &bbolt.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	service := &service{db: db}

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()
	protocol.RegisterProtocolServer(server, service)
	err = server.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}
