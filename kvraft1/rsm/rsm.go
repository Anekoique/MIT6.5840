package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const opTimeout = 100 * time.Millisecond

var useRaftStateMachine bool
var Unregister = struct{}{}

type Op struct {
	Me  int
	Id  int64
	Req any
}

// StateMachine is implemented by the replicated server (see server.go).
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type Waiter struct {
	id   int64
	term int
	ch   chan any
}

func (w *Waiter) notify(res any) {
	select {
	case w.ch <- res:
	default:
	}
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine

	pendings map[int]*Waiter

	shutdownCh   chan struct{}
	shutdownOnce sync.Once
}

func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendings:     make(map[int]*Waiter),
		shutdownCh:   make(chan struct{}),
	}
	snapshot := persister.ReadSnapshot()
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if maxraftstate >= 0 && len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}
	// Must hold lock before Start to ensure atomic registration of waiter,
	// preventing race where applier processes the entry before waiter is registered
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	waiter := &Waiter{id: op.Id, term: term, ch: make(chan any, 1)}
	rsm.registerWaiter(index, waiter)
	rsm.mu.Unlock()
	return rsm.waitForOp(index, waiter)
}

func (rsm *RSM) waitForOp(index int, waiter *Waiter) (rpc.Err, any) {
	ticker := time.NewTicker(opTimeout)
	defer ticker.Stop()
	defer rsm.unregisterWaiter(index, waiter)

	for {
		select {
		case res := <-waiter.ch:
			if res == Unregister {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res
		case <-ticker.C:
			if rsm.isTermStale(waiter.term) {
				return rpc.ErrWrongLeader, nil
			}
		case <-rsm.shutdownCh:
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) registerWaiter(index int, waiter *Waiter) {
	if old := rsm.pendings[index]; old != nil {
		old.notify(Unregister)
	}
	rsm.pendings[index] = waiter
}

func (rsm *RSM) unregisterWaiter(index int, waiter *Waiter) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if current, ok := rsm.pendings[index]; ok && current == waiter {
		delete(rsm.pendings, index)
	}
}

func (rsm *RSM) isTermStale(oldTerm int) bool {
	currentTerm, isLeader := rsm.rf.GetState()
	return !isLeader || currentTerm != oldTerm
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg)
		} else if msg.SnapshotValid {
			rsm.handleSnapshot(msg)
		}
	}
	rsm.abortPending()
}

func (rsm *RSM) handleCommand(msg raftapi.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok || op.Req == nil {
		return
	}

	result := rsm.sm.DoOp(op.Req)

	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if waiter, exists := rsm.pendings[msg.CommandIndex]; exists {
		if waiter.id == op.Id {
			waiter.notify(result)
		} else {
			waiter.notify(Unregister)
		}
	}
	if rsm.maxraftstate >= 0 && rsm.rf.PersistBytes() > int(float64(rsm.maxraftstate)*0.9) {
		go rsm.installSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) abortPending() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, waiter := range rsm.pendings {
		waiter.notify(Unregister)
	}
	clear(rsm.pendings)
	rsm.signalShutdown()
}

func (rsm *RSM) signalShutdown() {
	rsm.shutdownOnce.Do(func() {
		close(rsm.shutdownCh)
	})
}

func (rsm *RSM) installSnapshot(index int) {
	data := rsm.sm.Snapshot()
	rsm.rf.Snapshot(index, data)
}

func (rsm *RSM) handleSnapshot(msg raftapi.ApplyMsg) {
	rsm.sm.Restore(msg.Snapshot)
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for index, waiter := range rsm.pendings {
		if index <= msg.SnapshotIndex {
			waiter.notify(Unregister)
		}
	}
}
