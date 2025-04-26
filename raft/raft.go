package raft

import (
	"sync"

	"go_raft_kv/raft/common"
	"go_raft_kv/raft/rpc"
	"go_raft_kv/raft/state_machine_interface"
)

type NodeInfo struct {
	Addr   string        // 节点地址
	Status common.Status // 节点状态
	Alive  bool          // 节点是否存活
}

type Raft struct {
	mutex     sync.Mutex          // 互斥锁
	me        int                 // 当前节点id
	nodeInfos []NodeInfo          // 所有节点的信息，包括地址，身份，存活状态
	peers     []rpc.RaftRpcClient // 与其他节点通信的rpc入口
	status    common.Status       // 节点身份
	LeaderId  int                 // 当前leader id

	// 每个节点必须持久化（persistent）保存的状态
	currentTerm int               // 当前term
	votedFor    int               // 记录当前term给谁投票了
	logs        []common.LogEntry // 日志数组 第一个log.Index为1

	commitIndex int // 提交日志索引，表示单个节点可以被提交的最高日志索引
	lastApplied int // 最后一次应用给状态机log的index，它会将从 lastApplied+1 到 commitIndex 之间的所有日志条目应用到状态机中去，以确保状态机的一致性

	// leader维护 这两个状态的下标(是元素值不是物理下标)1开始，因为通常commitIndex和lastApplied从0开始，应该是一个无效的index，因此下标从1开始
	nextIndex  []int // 记录下一次将要发送给Follower的日志条目的索引
	matchIndex []int // 记录已经复制给该Follower的最高日志条目的索引，之前的已经全部被接收

	// 选举和心跳的时间
	lastElectionTime  int64 // 上次选举时间，收到心跳会更新
	lastHeartbeatTime int64 // 上次心跳时间

	// 日志快照 仅存储已经提交的log
	persister        state_machine_interface.Persister
	lastIncludeIndex int
	lastIncludeTerm  int

	// 重置状态机
	reset state_machine_interface.Reset

	// 上层客户端的指令 leader取
	clientCommands <-chan string

	// 已经被提交待应用的指令 状态机取
	applyCommands chan string

	// 状态机应用指令
	apply state_machine_interface.Apply

	// 判断是否是只读命令
	rwJudge state_machine_interface.RWJudge
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
