package raft

import (
	"go_raft_kv/raft/common"

	log "github.com/sirupsen/logrus"
)

func (r *Raft) startFetchCommand() {
	for command := range r.clientCommands {
		r.mutex.Lock()
		if r.status == common.Leader {
			log.Debugf("收到状态机命令：%v， 是leader节点，将其添加到日志中", command)
			lastLogIndex, _ := r.getLastLogIndexAndTerm()
			r.logs = append(r.logs, common.LogEntry{
				Command: command,
				Term:    r.currentTerm,
				Index:   lastLogIndex + 1,
			})
		} else {
			log.Debugf("收到状态机命令：%v， 不是leader节点，丢弃该命令", command)
		}
		r.mutex.Unlock()
	}
}
