package raft

import (
	"context"
	"time"

	"go_raft_kv/raft/common"
	"go_raft_kv/raft/rpc"

	log "github.com/sirupsen/logrus"
)

/*
* 心跳包的发送
* 1. leader节点定时发送心跳包给所有follower节点
* 2. 心跳包的发送频率是随机的，范围在100ms到300ms之间
* 3. 心跳包的发送是异步的，使用goroutine发送
 */
func (r *Raft) startHeartbeat() {
	for {
		r.mutex.Lock()
		if r.status == common.Leader {
			r.mutex.Unlock()
			log.Debugf("当前Leader节点id: %d, 当前term: %d, 发送心跳包", r.me, r.currentTerm)
			r.doHeartBeat()
		} else {
			r.mutex.Unlock()
		}
		time.Sleep(time.Duration(common.HeartbeatTimeout) * time.Millisecond) // 睡眠
	}
}

func (r *Raft) doHeartBeat() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	// 1. 遍历所有节点，发送心跳包
	// 即将要发送nextIndex[i]到lastLogIndex之间的log
	lastLogIndex, _ := r.getLastLogIndexAndTerm()
	for i := 0; i < len(r.peers); i++ {
		if i == r.me {
			continue
		}
		// prevLogIndex：要接在 follower 哪条日志之后追加。
		prevLogIndex := r.getServerPrevLogIndex(i)
		// 落后太久，使用快照同步
		if r.lastIncludeIndex > prevLogIndex {
			log.Warnf("节点落后太久，使用快照同步, server: %v,r.lastIncludeIndex: %v,prevLogIndex: %v", i, r.lastIncludeIndex, prevLogIndex)
			snapshot := r.Snapshot()
			log.Debugf("快照长度: %v", len(snapshot))
			req := &rpc.InstallSnapshotReq{
				Term:             int64(r.currentTerm),
				LeaderId:         int64(r.me),
				LastIncludeIndex: int64(r.lastIncludeIndex),
				LastIncludeTerm:  int64(r.lastIncludeTerm),
				Data:             snapshot,
			}
			go func(server int) {
				resp, err := r.peers[server].InstallSnapshot(context.Background(), req)
				if err != nil {
					log.Errorf("节点 %d 安装快照失败: %v", server, err)
					return
				}
				log.Debugf("发送快照心跳 节点id:%d req.Term:%d,LastIncludeIndex:%d,LastIncludeTerm:%d", server, req.Term, req.LastIncludeIndex, req.LastIncludeTerm)
				r.handleInstallSnapshotResp(r.lastIncludeIndex, server, resp)
			}(i)
		} else { // 使用日志同步
			// 2. 构造心跳包
			prevLogTerm := r.getLogTermByIndex(prevLogIndex)
			req := &rpc.AppendEntriesReq{
				Term:         int64(r.currentTerm),
				LeaderId:     int64(r.me),
				PrevLogIndex: int64(prevLogIndex),
				PrevLogTerm:  int64(prevLogTerm),
				Entries:      nil,
				LeaderCommit: int64(r.commitIndex), // 集群中已经被提交的最高索引
			}
			if lastLogIndex >= r.nextIndex[i] {
				// 发送从[r.nextIndex[i],lastLogIndex] 之间的log
				for j := r.nextIndex[i]; j <= lastLogIndex; j++ {
					sliceIndex := r.getSliceIndexByLogIndex(j)
					req.Entries = append(req.Entries, &rpc.LogEntry{
						Command: r.logs[sliceIndex].Command,
						Term:    int64(r.logs[sliceIndex].Term),
						Index:   int64(r.logs[sliceIndex].Index),
					})
				}
			}
			go func(server int) {
				// 3. 发送心跳包
				resp, err := r.peers[server].AppendEntries(context.Background(), req)
				if err != nil {
					log.Errorf("节点 %d 发送心跳包失败: %v", server, err)
					// 发送心跳包给follower节点失败，主观认为该follower节点不可用
					r.setNodeInfoDown(server)
					return
				}
				r.setNodeInfoAlive(server)
				log.Debugf("发送日志心跳 节点id:%d req.Term:%d,PrevLogIndex:%d,PrevLogTerm:%d,logSize:%d,LeaderCommit:%d", server, req.Term, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries), req.LeaderCommit)
				var sendLastLogIndex int
				if len(req.Entries) > 0 {
					sendLastLogIndex = int(req.Entries[len(req.Entries)-1].Index)
				}
				r.handleAppendEntriesResp(sendLastLogIndex, server, resp)
			}(i)
		}
	}
}

// leader处理appendEntries rpc的结果
func (r *Raft) handleAppendEntriesResp(sendLastLogIndex int, server int, resp *rpc.AppendEntriesResp) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.status == common.Follower {
		log.Debugf("我已经成为Follower，忽略收到的日志心跳响应")
		return
	}
	// 1. 如果term比当前term大，说明leader已经换了，更新当前term
	if int(resp.Term) > r.currentTerm {
		r.currentTerm = int(resp.Term)
		r.status = common.Follower
		r.setNodeInfoFollower(r.me)
		r.votedFor = -1
		r.saveState()
		log.Debugf("收到server:%d日志心跳回复，它的term:%d比我:%d的大，更新自己term，成为Follower", server, resp.Term, r.currentTerm)
		return
	}
	if resp.Success {
		// 为follower节点更新nextIndex和matchIndex
		if sendLastLogIndex == 0 { // 没有发送log，不更新nextIndex和matchIndex
			log.Debugf("收到server:%d日志心跳回复，成功。所有节点状态: %v", server, r.GetNodeInfos())
			return
		} else { // 发送了log，更新nextIndex和matchIndex
			r.nextIndex[server] = sendLastLogIndex + 1
			r.matchIndex[server] = sendLastLogIndex
			log.Debugf("收到server:%d日志心跳回复，成功。更新nextIndex和matchIndex, nextIndex:%d, matchIndex:%d", server, r.nextIndex[server], r.matchIndex[server])
			for n := sendLastLogIndex; n > 0; n-- { // 更新commitIndex
				if n <= r.commitIndex {
					break
				}
				// 如果我们当前考虑提交的日志条目 n 的 term 不等于当前 leader 的 term，那就 不能 通过“多数派复制”来提交它。
				if r.getLogTermByIndex(n) != r.currentTerm { // Raft永远不会通过对副本数计数的方式提交之前term的条目。只有leader当前term的日志条目才能通过对副本数计数的方式被提交
					break
				}
				matchCount := 0
				for i := 0; i < len(r.peers); i++ {
					if r.matchIndex[i] >= n {
						matchCount++
					}
				}
				matchCount++ // 执行++为什么？因为leader节点也算
				if matchCount >= len(r.peers)/2+1 {
					r.commitIndex = n
					// 应用指令
					r.applyLog()
					r.persist()
					r.saveState()
					log.Debugf("日志索引:%d, 超过半数match，提交索引并应用", r.commitIndex)
					break
				}
			}
			return
		}
	} else {
		// 可能日志不一致失败，递减nextIndex
		// 优化：Follower 在 AppendEntries 失败的响应中，附带告诉 Leader：“我这里 term 是什么，从哪个 index 开始冲突”。
		r.nextIndex[server]--
		log.Debugf("收到server:%d日志心跳回复，发生了日志不一样的错误，递减nextIndex:%d", server, r.nextIndex[server])
		return
	}
}

func (r *Raft) handleInstallSnapshotResp(sendLastLogIndex int, server int, resp *rpc.InstallSnapshotResp) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.status == common.Follower {
		log.Debugf("我已经成为Follower，忽略收到的快照心跳响应")
		return
	}
	if int(resp.Term) > r.currentTerm {
		r.currentTerm = int(resp.Term)
		r.status = common.Follower
		r.setNodeInfoFollower(r.me)
		r.votedFor = -1
		r.saveState()
		log.Debugf("收到server:%d快照心跳回复，它的term:%d比我:%d的大，更新自己term，成为Follower", server, resp.Term, r.currentTerm)
		return
	}
	// 为follower更新nextIndex和matchIndex
	r.nextIndex[server] = sendLastLogIndex + 1
	r.matchIndex[server] = sendLastLogIndex
	log.Debugf("收到server:%d快照心跳回复，成功，设置此server的nextIndex:%d, matchIndex:%d", server, r.nextIndex[server], r.matchIndex[server])
	for n := sendLastLogIndex; n > 0; n-- { // 更新commitIndex
		if n <= r.commitIndex {
			break
		}
		if r.getLogTermByIndex(n) != r.currentTerm { // Raft永远不会通过对副本数计数的方式提交之前term的条目。只有leader当前term的日志条目才能通过对副本数计数的方式被提交
			break
		}
		matchCount := 0
		for i := 0; i < len(r.peers); i++ {
			if r.matchIndex[i] >= n {
				matchCount++
			}
		}
		if matchCount >= len(r.peers)/2+1 {
			r.commitIndex = n
			// 应用指令
			r.applyLog()
			r.persist()
			r.saveState()
			log.Debugf("日志索引:%d, 超过半数match，提交索引并应用", r.commitIndex)
			break
		}
	}
	return
}

/*
* AppendEntries 心跳rpc
* follower节点接收到leader节点的心跳包并回包
 */
func (r *Raft) AppendEntries(ctx context.Context, req *rpc.AppendEntriesReq) (*rpc.AppendEntriesResp, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.lastElectionTime = time.Now().UnixMilli() // 收到心跳包，更新选举时间
	var resp *rpc.AppendEntriesResp
	if int(req.Term) < r.currentTerm { // leader日志过期
		resp.Term = int64(r.currentTerm)
		resp.Success = false
		log.Debugf("收到server:%d日志心跳，它的Term:%d比我的旧:%d，让它更新自己", req.LeaderId, req.Term, r.currentTerm)
		return resp, nil
	}

	if int(req.Term) > r.currentTerm { // leader日志更新
		r.currentTerm = int(req.Term)
		r.status = common.Follower
		r.setNodeInfoFollower(r.me)
		r.votedFor = -1
		r.saveState()
	}
	// req.Term == r.currentTerm
	r.LeaderId = int(req.LeaderId)
	r.setNodeInfoLeader(r.LeaderId) // 更新所有节点的身份
	lastLogIndex, _ := r.getLastLogIndexAndTerm()
	if req.PrevLogIndex != 0 {
		if lastLogIndex < int(req.PrevLogIndex) { // 本节点不包含prevLogIndex，返回false
			resp.Term = int64(r.currentTerm)
			resp.Success = false
			log.Debugf("收到server:%d日志心跳，prevLogIndex:%d还不存在，拒绝同步日志, lastLogIndex:%d", req.LeaderId, req.PrevLogIndex, lastLogIndex)
			return resp, nil
		}
		// 1. Follower 选错 Leader，之前日志来自不同 Leader
		// 2. Follower 崩溃恢复后，日志部分缺失或和 Leader 不一致
		// 3. 网络分区导致日志分叉
		term := r.getLogTermByIndex(int(req.PrevLogIndex))
		if int(req.PrevLogTerm) != term { // 日志term与prevLogTerm不符合，返回false
			resp.Term = int64(r.currentTerm)
			resp.Success = false
			log.Debugf("收到server:%d日志心跳，PrevLogTerm:%d不等于本节点相同位置处的Term:%d，日志不统一，拒绝同步日志", req.LeaderId, req.PrevLogTerm, term)
			return resp, nil
		}
	}
	if len(req.Entries) > 0 { // 日志不为空，更新日志
		var notExistSliceIndex = -1
		for i := 0; i < len(req.Entries); i++ { // 检查已经存在的日志条目和新的是否冲突
			if !r.isExistLogByIndex(int(req.Entries[i].Index)) { // 不存在
				notExistSliceIndex = i
				break
			}
			localTerm := r.getLogTermByIndex(int(req.Entries[i].Index))
			if int(req.Entries[i].Term) != localTerm { // 本地的entry和新的产生冲突，删除本地的entry及其只有所有的
				sliceIndex := r.getSliceIndexByLogIndex(int(req.Entries[i].Index))
				r.logs = r.logs[:sliceIndex]
				notExistSliceIndex = i
				break
			}
		}
		if notExistSliceIndex != -1 { // 有冲突，删除本地的entry及其后面的所有entry
			for i := notExistSliceIndex; i < len(req.Entries); i++ { // 附加尚未存在的新entry
				r.logs = append(r.logs, common.LogEntry{
					Command: req.Entries[i].Command,
					Term:    int(req.Entries[i].Term),
					Index:   int(req.Entries[i].Index),
				})
			}
		}
	}
	// 更新commitIndex
	if int(req.LeaderCommit) > r.commitIndex {
		lastLogIndex, _ = r.getLastLogIndexAndTerm()
		// 提交索引
		r.commitIndex = min(int(req.LeaderCommit), lastLogIndex)
		// 应用指令
		r.applyLog()
		r.persist()
		r.saveState()
		log.Debugf("日志索引:%d, 提交索引并应用", r.commitIndex)
	}
	resp.Term = int64(r.currentTerm)
	resp.Success = true
	log.Debugf("收到server:%d日志心跳，同意同步日志，所有节点状态: %v", req.LeaderId, r.GetNodeInfos())
	return resp, nil
}

/*
* InstallSnapshot 同步快照rpc
 */
func (r *Raft) InstallSnapshot(ctx context.Context, req *rpc.InstallSnapshotReq) (*rpc.InstallSnapshotResp, error) {
	var resp rpc.InstallSnapshotResp
	r.lastElectionTime = time.Now().UnixMilli() // 收到心跳，重置选举时间
	if int(req.Term) < r.currentTerm {
		resp.Term = int64(r.currentTerm)
		log.Debugf("收到server:%d快照心跳，它的Term:%d比我的旧:%d，让它更新自己", req.LeaderId, req.Term, r.currentTerm)
		return &resp, nil
	} else if int(req.Term) > r.currentTerm {
		r.currentTerm = int(req.Term)
		r.status = common.Follower
		r.setNodeInfoFollower(r.me)
		r.votedFor = -1
		r.saveState()
	}
	// term == r.currentTerm
	if r.lastIncludeIndex != int(req.LastIncludeIndex) || r.lastIncludeTerm != int(req.LastIncludeTerm) { // 丢掉logs
		r.logs = nil
	}
	r.replaceSnapshot(req.Data) // 丢弃旧的快照，存储新的快照
	r.lastIncludeIndex = int(req.LastIncludeIndex)
	r.lastIncludeTerm = int(req.LastIncludeTerm)
	r.saveState()
	// 重置状态机
	r.Reset()
	r.commitIndex = r.lastIncludeIndex
	r.lastApplied = r.lastIncludeIndex
	// 响应
	resp.Term = int64(r.currentTerm)
	log.Debugf("收到server:%d快照心跳，成功同步快照，commitIndex:%d", req.LeaderId, r.commitIndex)
	return &resp, nil
}

/*
* 获取某个 follower 的上一条日志索引
 */
func (r *Raft) getServerPrevLogIndex(server int) (prevLogIndex int) {
	prevLogIndex = r.nextIndex[server] - 1
	return
}

/*
* 快照覆盖了日志 1~100，那么你只保留从 101 以后 的日志在内存 log []Entry 中；
* getSliceIndexByLogIndex的作用是将日志索引转换为切片索引
 */
func (r *Raft) getSliceIndexByLogIndex(logIndex int) (sliceIndex int) {
	return logIndex - r.lastIncludeIndex - 1
}

/*
* 判断日志条目是否存在
 */
func (r *Raft) isExistLogByIndex(logIndex int) bool {
	if logIndex <= 0 {
		return false
	}
	if i := r.getSliceIndexByLogIndex(logIndex); i >= len(r.logs) {
		return false
	}
	return true
}

// 确保索引存在
func (r *Raft) getLogTermByIndex(logIndex int) int {
	if logIndex == 0 {
		return 0
	}
	if logIndex == r.lastIncludeIndex {
		return r.lastIncludeTerm
	}
	return r.logs[r.getSliceIndexByLogIndex(logIndex)].Term
}
