package state_machine_interface

type Persister interface {
	Persist(data []string) error       // 数据应该递增存储
	ReplaceSnapshot(data []byte) error // 替换快照
	Snapshot() ([]byte, error)         // 获取快照
	SaveState(state []byte) error      // 保存状态
	ReadState() ([]byte, error)        // 读取状态
}
