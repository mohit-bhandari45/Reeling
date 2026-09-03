package job

import "time";

type Status string

const (
	StatusQueued Status = "queued"
	StatusProcessing Status = "processing"
	StatusDone Status = "done"
	StatusFailed Status = "failed"
)

type Job struct {
	ID string `json:"id"`
	InputKey string `json:"input_key"`
	OutputKeys []string `json:"output_keys,omitempty"`
	Status Status `json:"status"`
	Error string `json:"error,omitempty"`
	Attempts int `json:"attempts"`
	WebhookURL  string    `json:"webhook_url,omitempty"`
	Renditions  []string  `json:"renditions,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store interface {
	Save(j *Job) error
	Get(id string) (*Job, error)
}