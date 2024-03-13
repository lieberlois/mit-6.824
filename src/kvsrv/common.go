package kvsrv

type PutAppendMode int

const (
	Write PutAppendMode = iota
	ReportSuccess
)

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	RequestId int64
	Mode      PutAppendMode
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

type ReportSuccessArgs struct {
	RequestId int64
}
