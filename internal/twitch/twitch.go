package twitch

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

const noPlayableStreams = "No playable streams found"

type commandRunner interface {
	Run(ctx context.Context, name string, args ...string) (stdout, stderr []byte, err error)
}

type execCommandRunner struct{}

func (execCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

type Client struct {
	streamlinkPath string
	runner         commandRunner
}

func NewClient(streamlinkPath string) *Client {
	return newClient(streamlinkPath, execCommandRunner{})
}

func newClient(streamlinkPath string, runner commandRunner) *Client {
	return &Client{streamlinkPath: streamlinkPath, runner: runner}
}

func (c *Client) IsOnline(ctx context.Context, channel string) (bool, error) {
	url := "https://www.twitch.tv/" + channel
	stdout, stderr, err := c.runner.Run(ctx, c.streamlinkPath,
		"--stream-url",
		"--loglevel", "error",
		url,
		"best",
	)
	if err == nil {
		return true, nil
	}
	if bytes.Contains(stdout, []byte(noPlayableStreams)) || bytes.Contains(stderr, []byte(noPlayableStreams)) {
		return false, nil
	}

	details := strings.TrimSpace(string(stderr))
	if details == "" {
		return false, fmt.Errorf("streamlink status check for %q: %w", channel, err)
	}
	return false, fmt.Errorf("streamlink status check for %q: %w: %s", channel, err, details)
}
