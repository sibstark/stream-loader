package twitch

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type fakeCommandRunner struct {
	stdout []byte
	stderr []byte
	err    error
	name   string
	args   []string
}

func (f *fakeCommandRunner) Run(_ context.Context, name string, args ...string) ([]byte, []byte, error) {
	f.name = name
	f.args = append([]string(nil), args...)
	return f.stdout, f.stderr, f.err
}

func TestClientReportsOnlineAndBuildsStreamlinkProbe(t *testing.T) {
	runner := &fakeCommandRunner{stdout: []byte("https://video.example/live.m3u8\n")}
	client := newClient("/usr/local/bin/streamlink", runner)

	online, err := client.IsOnline(context.Background(), "Shroud")
	if err != nil {
		t.Fatalf("IsOnline() error = %v", err)
	}
	if !online {
		t.Fatal("IsOnline() = false, want true")
	}
	wantArgs := []string{"--stream-url", "--loglevel", "error", "https://www.twitch.tv/Shroud", "best"}
	if runner.name != "/usr/local/bin/streamlink" || !reflect.DeepEqual(runner.args, wantArgs) {
		t.Fatalf("command = %q %q, want %q %q", runner.name, runner.args, "/usr/local/bin/streamlink", wantArgs)
	}
}

func TestClientReportsOfflineOnlyForNoPlayableStreams(t *testing.T) {
	runner := &fakeCommandRunner{
		stderr: []byte("error: No playable streams found on this URL: https://www.twitch.tv/shroud\n"),
		err:    errors.New("exit status 1"),
	}
	client := newClient("streamlink", runner)

	online, err := client.IsOnline(context.Background(), "shroud")
	if err != nil {
		t.Fatalf("IsOnline() error = %v", err)
	}
	if online {
		t.Fatal("IsOnline() = true, want false")
	}
}

func TestClientReportsOfflineWhenNoPlayableStreamsIsWrittenToStdout(t *testing.T) {
	runner := &fakeCommandRunner{
		stdout: []byte("error: No playable streams found on this URL: https://www.twitch.tv/dvshkaa\n"),
		err:    errors.New("exit status 1"),
	}
	client := newClient("streamlink", runner)

	online, err := client.IsOnline(context.Background(), "dvshkaa")
	if err != nil {
		t.Fatalf("IsOnline() error = %v", err)
	}
	if online {
		t.Fatal("IsOnline() = true, want false")
	}
}

func TestClientPreservesOtherStreamlinkFailuresAsErrors(t *testing.T) {
	runner := &fakeCommandRunner{
		stderr: []byte("error: Unable to open URL: connection timed out\n"),
		err:    errors.New("exit status 1"),
	}
	client := newClient("streamlink", runner)

	online, err := client.IsOnline(context.Background(), "shroud")
	if err == nil {
		t.Fatal("IsOnline() error = nil, want error")
	}
	if online {
		t.Fatal("IsOnline() = true, want false")
	}
	if !strings.Contains(err.Error(), "connection timed out") {
		t.Fatalf("IsOnline() error = %q, want stderr details", err)
	}
}
