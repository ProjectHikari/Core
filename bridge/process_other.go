//go:build !unix

package bridge

import (
	"errors"
	"os"
	"os/exec"
)

func prepareProcess(cmd *exec.Cmd) {}

func killProcess(cmd *exec.Cmd) error {
	err := cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
