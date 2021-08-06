package mr

import "fmt"

type TaskPhase int

const(
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct{
	fileName string
	NReduce int
	NMaps int
	taskId int
	phase TaskPhase
	alive bool
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}