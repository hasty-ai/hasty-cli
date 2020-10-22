package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func exitOnErr(err error) {
	if err == nil {
		return
	}
	log.Error(err)
	os.Exit(1)
}
