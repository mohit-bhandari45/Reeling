package ffmpeg

import (
	"context"
	"fmt"
	"os/exec"
)

type Runner struct {
	WorkDir string
}

func NewRunner(workDir string) *Runner {
	return &Runner{WorkDir: workDir}
}

func (r *Runner) Transcode(ctx context.Context, input, output, preset string) error {
	cmd := exec.CommandContext(ctx, "ffmpeg", 
		"-i", input,
		"-c:v", "libx264",
		"-preset", preset,
		"-c:a", "aac",
		"-y", output,
	)

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