package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrHandleTimeout = "ErrHandleTimeout"
	ErrChanClosed    = "ErrChanClosed"
	ErrLeaderChanged = "ErrLeaderChanged"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IncrId  uint64
	ClerkId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	IncrId  uint64
	ClerkId int
}

type GetReply struct {
	Err   Err
	Value string
}
