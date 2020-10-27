package create

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/hasty-ai/hasty-go"
)

const httpTimeout = 5 * time.Second

type config struct {
	Project  string
	Name     string
	HastyKey string `envconfig:"HASTY_API_KEY" required:"true"`
}

type cmd struct {
	config config
}

func (i *cmd) run(cmd *cobra.Command, args []string) {
	if err := envconfig.Process("", &i.config); err != nil {
		log.Fatalf("Unable to read configuration from environment variables: %s", err)
	}

	hc := hasty.NewClient(i.config.HastyKey)
	ctx, cancel := context.WithTimeout(cmd.Context(), httpTimeout)
	defer cancel()

	params := &hasty.DatasetParams{
		Project: &i.config.Project,
		Name:    &i.config.Name,
	}
	ds, err := hc.Dataset.New(ctx, params)
	if err != nil {
		log.Fatalf("Unable to create dataset: %s", err)
	}

	buff, err := json.Marshal(ds)
	if err != nil {
		log.Fatalf("Unable to create dataset: %s", err)
	}
	fmt.Println(string(buff))
}
