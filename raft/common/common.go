package common

type Status int

const (
	Leader    = 1 // 领导者
	Follower  = 2 // 跟随者
	Candidate = 3 // 候选者

	ElectionBaseTimeout     = 1500 // 选举基准超时时间（ms）
	ElectionMaxExtraTimeout = 1500 // 选举额外最大超时时间（ms）
	HeartbeatInterval       = 250  // 心跳间隔（ms）

	RpcTimeout = 2000 // RPC超时时间（ms）
)

type LogEntry struct {
	Command string // 日志条目
	Term    int    // 日志条目所属的任期
	Index   int    // 日志条目的索引
}

// State 代表 Raft 节点的状态
type State struct {
	CurrentTerm       int // 当前任期
	VotedFor          int // 投票给的候选人ID
	LastIncludedIndex int // 最后包含的日志条目的索引
	LastIncludedTerm  int // 最后包含的日志条目的任期
}
