package main

import (
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/hasty-ai/cli/cmd"
)

func main() {
	rootCmd := cmd.BuildCmdTree()
	if err := rootCmd.Execute(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
