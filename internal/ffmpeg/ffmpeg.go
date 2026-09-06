package ffmpeg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Runner struct {}

func NewRunner() *Runner {
	return &Runner{}
}

// resolution heights struct
var resolutionHeights = map[string]string {
	"1080p": "1080",
	"720p":  "720",
	"480p":  "480",
	"360p":  "360",
}

// Master playlist file structs
var renditionBandWidth = map[string]int {
	"1080p": 5000000,
	"720p":  2800000,
	"480p":  1400000,
	"360p":  800000,
}

var resolutionDimensions = map[string][2]int{
	"1080p": {1920, 1080},
	"720p":  {1280, 720},
	"480p":  {854, 480},
	"360p":  {640, 360},
}

func RenditionBandwidth(res string) int {
	return renditionBandWidth[res]
}

func ResolutionDimensions(res string) (width, height int){
	dims := resolutionDimensions[res];
	return dims[0], dims[1];
}

// every rendition will have renditioninfo to check
type RenditionInfo struct {
	Name string
	Bandwidth int
	Width int
	Height int
}

func (r *Runner) Transcode(ctx context.Context, input, output, preset, resolution string) error {
	args := []string {
		"-i", input,
		"-c:v", "libx264",
		"-preset", preset,
	}

	if resolution != "" {
		height, ok := resolutionHeights[resolution]
		if !ok {
			return fmt.Errorf("unknown resolution: %s", resolution)
		}
		args = append(args, "-vf", fmt.Sprintf("scale=-2:%s", height));
	}

	args = append(args, "-c:a", "aac","-y", output);

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	out, err := cmd.CombinedOutput();
	if err != nil {
		return fmt.Errorf("ffmpeg transcode failed: %w\n%s", err, out);
	}
	return nil
}

func (r *Runner) Thumbnail(ctx context.Context, input, output string, atSeconds int) error {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-ss", fmt.Sprintf("%d", atSeconds),
		"-i", input,
		"-frames:v", "1",
		"-y", output,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg thumbnail failed: %w\n%s", err, out)
	}
	return nil
}

func (r *Runner) TranscodeHLS(ctx context.Context, input, outputDir, resolution string) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err);
	}

	args := []string {
		"-i", input,
		"-c:v", "libx264",
		"-c:a", "aac",
	}

	if resolution != "" {
		height, ok := resolutionHeights[resolution]
		if !ok {
			return fmt.Errorf("unknown resolution: %s", resolution);
		}
		args = append(args, "-vf", fmt.Sprintf("scale=-2:%s", height));
	}

	args = append(args, 
		"-hls_time", "6",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", filepath.Join(outputDir, "segment%03d.ts"),
		"-y", filepath.Join(outputDir, "playlist.m3u8"),
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...);

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg hls transcode failed: %w\n%s", err, out);
	}

	return nil;
}

func BuildMasterPlaylist(renditiosInfo []RenditionInfo) string {
	var sb strings.Builder;
	sb.WriteString("#EXTM3U\n");

	for _, r := range renditiosInfo {
		sb.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/playlist.m3u8\n",
			r.Bandwidth, r.Width, r.Height, r.Name,
		))
	}

	return sb.String();
}