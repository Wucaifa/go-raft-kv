package raft

import log "github.com/sirupsen/logrus"

// 应用状态机，从r.lastApplied 到 r.commitIndex
func (r *Raft) applyLog() {
	log.Infof("执行应用日志... 从lastApplied+1=%d 到 commitIndex=%d", r.lastApplied+1, r.commitIndex)
	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		log.Infof("日志索引: %d", i)
		// 这里可以添加应用状态机的逻辑
		// 例如：r.apply.Apply(r.logs[i].Command)
		applyLogSliceIndex := r.getSliceIndexByLogIndex(i)
		command := r.logs[applyLogSliceIndex].Command
		term := r.logs[applyLogSliceIndex].Term
		r.apply.ApplyCommand(command)
		log.Infof("应用日志: %v, term: %v, command: %v", i, term, command)
	}
	log.Infof("执行应用日志结束...")
	r.lastApplied = r.commitIndex
}
