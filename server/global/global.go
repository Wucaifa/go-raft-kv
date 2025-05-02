package global

import (
	kv_raft "go_raft_kv"
	"go_raft_kv/raft"
	"go_raft_kv/server/storage"
)

var (
	Config         *kv_raft.Config                // 配置文件
	R              *raft.Raft                     // Raft 实例
	ClientCommands chan string                    // 客户端命令通道
	StorageEngine  storage.StorageEngineInterface // 存储引擎
	Reqs           = make(map[string]chan string) // 请求通道
)

const (
	Success = "success" // 成功
	Failed  = "failed"  // 失败
	Forward = "forward" // 转发
)
