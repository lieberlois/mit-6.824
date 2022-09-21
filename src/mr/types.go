package mr

type TaskType int
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
	Exit
	Sleep
)

type Task struct {
	Id       int
	Type     TaskType
	State    TaskState
	File     string
	WorkerId int
}
