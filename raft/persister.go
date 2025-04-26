package raft

import (
	"encoding/json"
	"fmt"

	"go_raft_kv/raft/common"

	log "github.com/sirupsen/logrus"
)

func (r *Raft) persist() {
	// 持久化到commitIndex
	var data []string
	commitSliceIndex := r.getSliceIndexByLogIndex(r.commitIndex)
	// 取出下标从 0 到 commitSliceIndex（包含）的所有元素。
	for _, entry := range r.logs[:commitSliceIndex+1] {
		data = append(data, entry.Command)
	}

	if err := r.persister.Persist(data); err != nil {
		panic(err)
	}
	r.lastIncludeIndex = r.logs[commitSliceIndex].Index
	r.lastIncludeTerm = r.logs[commitSliceIndex].Term
	r.saveState()
	r.logs = r.logs[commitSliceIndex+1:]
}

// 当日志过多时，为了节省存储空间和提高效率，会生成快照（snapshot）来替代旧日志。
func (r *Raft) replaceSnapshot(data []byte) {
	// 替换快照
	if err := r.persister.ReplaceSnapshot(data); err != nil {
		panic(err) // 直接 panic(err) 是开发阶段常见写法
		// log.Errorf("failed to replace snapshot: %v", err)
		// return // 或执行降级处理
	}
}

func (r *Raft) snapshot() []byte {
	// 获取快照
	snapshot, err := r.persister.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

func (r *Raft) saveState() {
	// 保存状态
	state := common.State{
		CurrentTerm:      r.currentTerm,
		VotedFor:         r.votedFor,
		LastIncludeIndex: r.lastIncludeIndex,
		LastIncludeTerm:  r.lastIncludeTerm,
	}
	// 将状态序列化为 JSON 格式
	data, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	if err := r.persister.SaveState(data); err != nil {
		panic(err)
	}
}

func (r *Raft) readState() (*common.State, error) {
	// 读取状态
	data, err := r.persister.ReadState()
	if err != nil {
		return nil, err
	}
	log.Infof("最新读取状态: %s", string(data))
	var state common.State
	// 如果数据格式不正确，返回错误
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("json.Unmarshal err: %v", err)
	}
	return &state, nil

}
