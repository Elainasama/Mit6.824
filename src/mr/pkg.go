package mr

import (
	"time"
)

type JobType int

const (
	MapTask JobType = iota
	ReduceTask
	WaitingTask
	FinishTask
)

// Task 结构体包含文件存放位置，以及任务的类别
type Task struct {
	Filename  string
	Status    JobType
	StartTime time.Time
}

type Schedule int

const (
	MapSchedule Schedule = iota
	ReduceSchedule
	FinishSchedule
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
