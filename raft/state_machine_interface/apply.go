package state_machine_interface

type Apply interface {
	// ApplyCommand 应用命令
	ApplyCommand(command string) string
}
