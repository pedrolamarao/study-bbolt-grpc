package main

import (
	"context"
	"errors"
	"log"
	"net"

	"go.etcd.io/bbolt"

	"google.golang.org/grpc"
	"purpura.dev.br/study/bbolt-grpc/protocol"
)

type service struct {
	protocol.UnimplementedValueServer
	protocol.UnimplementedBucketServer

	db *bbolt.DB
}

func (srv *service) GetValue(_ context.Context, request *protocol.GetValueRequest) (*protocol.GetValueResponse, error) {
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
	response := &protocol.GetValueResponse_builder{
		Value: value,
	}
	return response.Build(), nil
}

func (srv *service) SetValue(_ context.Context, request *protocol.SetValueRequest) (*protocol.SetValueResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(request.GetBucket())
		if bucket == nil {
			return errors.New("bucket not found")
		}
		err := bucket.Put(request.GetKey(), request.GetValue())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.SetValueResponse_builder{}
	return response.Build(), nil
}

func (srv *service) DeleteValue(_ context.Context, request *protocol.DeleteValueRequest) (*protocol.DeleteValueResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(request.GetBucket())
		if bucket == nil {
			return errors.New("bucket not found")
		}
		err := bucket.Delete(request.GetKey())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.DeleteValueResponse_builder{}
	return response.Build(), nil
}

func (srv *service) CreateBucket(_ context.Context, request *protocol.CreateBucketRequest) (*protocol.CreateBucketResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket(request.GetBucket())
		return err
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.CreateBucketResponse_builder{}
	return response.Build(), nil
}

func (srv *service) DestroyBucket(_ context.Context, request *protocol.DestroyBucketRequest) (*protocol.DestroyBucketResponse, error) {
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket(request.GetBucket())
	})
	if err != nil {
		return nil, err
	}
	response := &protocol.DestroyBucketResponse_builder{}
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
	protocol.RegisterValueServer(server, service)
	protocol.RegisterBucketServer(server, service)
	err = server.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}
