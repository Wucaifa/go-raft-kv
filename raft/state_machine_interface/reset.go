package state_machine_interface

type Reset interface {
	Reset() error // 重置状态机
}
