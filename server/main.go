package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	kv_raft "go_raft_kv"
	"go_raft_kv/raft"
	"go_raft_kv/server/command"
	"go_raft_kv/server/components/apikey"
	"go_raft_kv/server/components/httptool"
	"go_raft_kv/server/handler"
	"go_raft_kv/server/impl"
	"go_raft_kv/server/storage"

	"go_raft_kv/server/global"
)

func mian() {
	// 解析命令行参数 mykv-server -f config.yaml
	app := cli.NewApp()
	app.Name = "mykv-server"                      // 应用名称
	app.Usage = "kv storage system based on raft" // 应用描述
	app.Flags = []cli.Flag{                       // 命令行参数
		cli.StringFlag{
			Name:  "f",
			Usage: "-f config file",
		},
	}

	// app.Before 是在整个 app.Run() 执行过程中，第一个被执行的东西，用来做程序启动前的准备工作！
	app.Before = func(c *cli.Context) error {
		log.SetFormatter(&log.JSONFormatter{}) // 设置日志输出格式为 JSON格式
		log.SetOutput(os.Stdout)               // 设置日志输出到标准输出（也就是命令行窗口）
		log.SetLevel(log.DebugLevel)           // 设置日志级别为 DebugLevel
		return nil
	}

	app.Action = func(c *cli.Context) {
		// 读取配置文件
		configFile := c.String("f")
		config, err := kv_raft.ReadConfigFile(configFile)
		if err != nil {
			log.Errorf("kv_raft.ReadConfigFile err: %v", err)
			return
		}
		log.Debugf("config loaded: %v.", config)
		if err = Server(config); err != nil {
			log.Errorf("server run failed: %v", err)
		}
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func Server(config *kv_raft.Config) error {
	// 创建持久化的文件描述符，存储raft的日志
	// 这里的持久化是指将数据写入到磁盘中，以便在系统重启后仍然可以访问这些数据
	logFile, err := os.OpenFile(config.LogStorageFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("os.OpenFile err: %v", err)
	}
	defer logFile.Close()

	stateFile, err := os.OpenFile(config.StateStorageFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("os.OpenFile err: %v", err)
	}
	defer stateFile.Close()

	// 初始化持久化实现
	persister := impl.NewFilePersister(logFile, stateFile)
	// 初始化重置实现
	reset := impl.NewReset(logFile)
	// 初始化读写判断命令实现
	rwJudge := impl.NewRWJudge()
	// 初始化应用指令实现
	apply := impl.NewApplyCommand()
	// 创建客户端命令通道
	global.ClientCommands = make(chan string)
	// 初始化存储引擎
	global.StorageEngine = storage.NewStorageEngine("gomap")
	// 存储全局配置
	global.Config = config
	// 创建raft节点
	global.R, err = raft.NewRaft(config.Me, config.GetRaftAddrs(), persister, reset, rwJudge, apply, false, global.ClientCommands)
	if err != nil {
		return fmt.Errorf("raft.NewRaft err: %v", err)
	}

	// 启动raft
	global.R.Start()
	// 初始化root账号
	InitRoot()
	// 创建http Server
	if err = RunKVServer(config); err != nil {
		return fmt.Errorf("RunKVServer err: %v", err)
	}
	return nil
}

func RunKVServer(config *kv_raft.Config) error {
	log.Info("开始执行server启动", config.Addrs[config.Me].Server)
	http.HandleFunc("/receive", handler.ReceiveHandler)
	http.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		body, err := httptool.StringBody(r)
		if err != nil {
			httptool.ErrorResponse(w, err.Error())
		}
		body = strings.TrimSpace(body)
		var jsonData []byte
		v, _ := global.StorageEngine.Get(body)
		kv := command.KV{Key: body, Value: v}
		jsonData, _ = json.Marshal(kv)
		w.Write(jsonData)
	})
	http.HandleFunc("/debug-allkey", func(w http.ResponseWriter, r *http.Request) {
		kvs := make([]command.KV, 0)
		keys := global.StorageEngine.Prefix("")
		for i := 0; i < len(keys); i++ {
			value, _ := global.StorageEngine.Get(keys[i])
			kvs = append(kvs, command.KV{
				Key:   keys[i],
				Value: value,
			})
		}
		jsonData, _ := json.Marshal(kvs)
		w.Write(jsonData)
	})

	// 启动
	if err := http.ListenAndServe(config.Addrs[config.Me].Server, nil); err != nil {
		return fmt.Errorf("http.ListenAndServe err: %v", err)
	}
	return nil
}

func InitRoot() {
	// 等待领导者选举完成
	for global.R.LeaderId == -1 {
		log.Error("领导者选举没有完成: ", global.R.LeaderId)
		time.Sleep(time.Second)
	}
	if global.R.LeaderId != global.R.GetMeId() {
		log.Infof("领导者选举完成，自己不是领导者，不用初始化root")
		return
	}
	// 检查有没有root账号
	aclGetExactCmdStr := command.ExactCmdStr(command.Normalize("acl-get root"))
	aclGetExactCmd := command.Unmarshal(aclGetExactCmdStr)
	var acl command.AclGetResponse
	jsonData := aclGetExactCmd.ExecCMD()
	json.Unmarshal([]byte(jsonData), &acl)
	if acl.Exist {
		log.Infof("领导者选举完成，我是Leader，已有root账号，不需要初始化: %s", []byte(jsonData))
		return
	}
	// 没有则创建root账号
	aclAddExactCmdStr := command.ExactCmdStr(command.Normalize("acl-add root *,rw " + apikey.GenerateApiKey(32)))
	// 将命令发送给raft
	requestId := uuid.New().String()
	global.Reqs[requestId] = make(chan string)
	raftCmd := string(aclAddExactCmdStr) + " " + requestId
	global.ClientCommands <- raftCmd
	result := <-global.Reqs[requestId]
	log.Infof("初始化root账号完成 apikey: %s", result)
}
