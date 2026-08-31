package ffmpeg

import (
	"context"
	"testing"
)

func TestTranscodeManual(t *testing.T) {
	r := NewRunner(".")
	err := r.Transcode(context.Background(), "sample.mp4", "out.mp4", "medium")
	if err != nil {
		t.Fatal(err)
	}
}