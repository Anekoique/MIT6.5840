package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const UnLocked string = ""

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	key      string
	clientID string
}

// MakeLock passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		key:      l,
		clientID: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, ok := lk.ck.Get(lk.key)
		if ok != rpc.OK && ok != rpc.ErrNoKey || value != UnLocked {
			continue
		}

		if ok := lk.ck.Put(lk.key, lk.clientID, version); ok != rpc.OK && ok != rpc.ErrMaybe {
			continue
		}

		value, _, ok = lk.ck.Get(lk.key)
		if ok == rpc.OK && value == lk.clientID {
			break
		}
	}
}

func (lk *Lock) Release() {
	for {
		_, version, ok := lk.ck.Get(lk.key)
		if ok != rpc.OK {
			continue
		}

		ok = lk.ck.Put(lk.key, UnLocked, version)
		if ok == rpc.OK {
			return
		}
		if ok == rpc.ErrMaybe {
			if _, update, _ := lk.ck.Get(lk.key); update >= version+1 {
				return
			}
		}
	}
}
