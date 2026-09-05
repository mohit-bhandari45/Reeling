package ffmpeg

import (
	"context"
	"testing"
)

func TestTranscodeManual(t *testing.T) {
	r := NewRunner()
	err := r.Transcode(context.Background(), "sample.mp4", "out.mp4", "medium", "")
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

func TestTranscodeHLSManual(t *testing.T) {
	r := NewRunner()
	err := r.TranscodeHLS(context.Background(), "sample.mp4", "hls_output/720p", "720p")
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuildMasterPlaylistManual(t *testing.T) {
	playlist := BuildMasterPlaylist([]RenditionInfo{
		{Name: "1080p", Bandwidth: 5000000, Width: 1920, Height: 1080},
		{Name: "720p", Bandwidth: 2800000, Width: 1280, Height: 720},
		{Name: "480p", Bandwidth: 1400000, Width: 854, Height: 480},
	})
	t.Logf("\n%s", playlist)
}