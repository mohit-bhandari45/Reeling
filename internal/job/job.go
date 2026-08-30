package job

import "time";

type Status string

const (
	StatusQueued Status = "queued"
	StatusProcessing Status = "processing"
	StatusDone Status = "done"
	StatusFailed Status = "failed"
)

