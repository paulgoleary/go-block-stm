package block_stm

import "sync/atomic"

type SchedulerTask interface{}

type SchedulerTaskDone struct{}

type Scheduler struct {
	executionIdx  uint32
	validationIdx uint32
	decreaseCnt   uint32
}

func MakeScheduler() *Scheduler {
	return &Scheduler{
		executionIdx:  0,
		validationIdx: 0,
		decreaseCnt:   0,
	}
}

func (s *Scheduler) Done() bool {
	return false // TODO
}

func (s *Scheduler) NextTask() SchedulerTask {
	for {
		if s.Done() {
			return SchedulerTaskDone{}
		}

		if atomic.LoadUint32(&s.validationIdx) < atomic.LoadUint32(&s.executionIdx) {

		}
	}
}

func (s *Scheduler) tryValidateNextVersion() {

}
