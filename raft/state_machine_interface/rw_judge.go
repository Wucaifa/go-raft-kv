package state_machine_interface

type RWJudge interface {
	ReadOnly(command string) bool // 判断是否是只读命令
}
