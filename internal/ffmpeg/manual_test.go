package ffmpeg

import (
	"context"
	"testing"
)

func TestTranscodeManual(t *testing.T) {
	r := NewRunner()
	err := r.Transcode(context.Background(), "sample.mp4", "out.mp4", "medium")
	if err != nil {
		t.Fatal(err)
	}
}

func TestThumbnailManual(t *testing.T) {
	r := NewRunner()
	err := r.Thumbnail(context.Background(), "sample.mp4", "thumb.jpg", 1)
	if err != nil {
		t.Fatal(err)
	}
}