package ffmpeg

import (
	"context"
	"fmt"
	"os/exec"
)

type Runner struct {}

func NewRunner() *Runner {
	return &Runner{}
}

var resolutionHeights = map[string]string {
	"1080p": "1080",
	"720p":  "720",
	"480p":  "480",
	"360p":  "360",
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