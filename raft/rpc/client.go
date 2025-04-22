package rpc

import (
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewRpcClient(addr string) RaftRpcClient {
	// 创建一个 gRPC 客户端连接
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Errorf("collect addr %s failed: %v, retry...", addr, err)
		panic(err)
	}
	client := NewRaftRpcClient(conn)
	return client
}
