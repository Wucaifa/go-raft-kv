package raft

import (
	"sync"

	"go-raft-kv/raft/common"
	"go-raft-kv/raft/rpc"
	"go-raft-kv/raft/state_machine_interface"
)

type NodeInfo struct {
	Addr   string        // 节点地址
	Status common.Status // 节点状态
	Alive  bool          // 节点是否存活
}

type Raft struct {
	mutex     sync.Mutex          // 互斥锁
	me        int                 // 当前节点id
	nodeInfos []NodeInfo          // 节点信息
	peers     []rpc.RaftRpcClient // 与其他节点通信的rpc入口
	Status    common.Status       // 节点身份
	LeaderId  int                 // 当前leader id

	//
	currentTerm int
	votedFor    int
	logs        []common.LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// 选举和心跳的时间
	lastElectionTime  int64
	lastHeartbeatTime int64

	// 日志快照 仅存储已经提交的log
	persister        state_machine_interface.Persister
	lastIncludeIndex int
	lastIncludeTerm  int

	// 重置状态机
	reset state_machine_interface.reset

	// 上层客户端的指令 leader取
	clientCommands <-chan string

	// 已经被提交待应用的指令 状态机取
	applyCommands chan string

	// 状态机应用指令
	apply state_machine_interface.apply

	// 判断是否是只读命令
	rwJudge state_machine_interface.RwJudge
	rpc.UnimplementedRaftRpcServer
}

func (r *Raft) GetMeId() int {
	return r.me
}

func (r *Raft) GetNodeInfos() []NodeInfo {
	return r.nodeInfos
}

func (r *Raft) GetNodeInfo() NodeInfo {
	return r.nodeInfos[r.me]
}

func (r *Raft) GetNodeLeaderInfo() NodeInfo {
	return r.nodeInfos[r.LeaderId]
}

func (r *Raft) setNodeInfoCandiate() {
	for i := 0; i < len(r.nodeInfos); i++ {
		r.nodeInfos[i].Status = common.Candidate
	}
}

func (r *Raft) setNodeInfoDown(i int) {
	r.nodeInfos[i].Alive = false
}

func (r *Raft) setNodeInfoAlive(i int) {
	r.nodeInfos[i].Alive = true
}

func (r *Raft) setNodeInfoLeader(leaderId int) {
	for i := 0; i < len(r.nodeInfos); i++ {
		if i == leaderId {
			r.nodeInfos[i].Status = common.Leader
		} else {
			r.nodeInfos[i].Status = common.Follower
		}
	}
}

func (r *Raft) setNodeInfoFollower(followerId int) {
	r.nodeInfos[followerId].Status = common.Follower
}

func (r *Raft) getLastLogIndexAndTerm() (int, int) {
	if len(r.logs) == 0 {
		return -1, -1
	}
	return r.logs[len(r.logs)-1].Index, r.logs[len(r.logs)-1].Term
}
