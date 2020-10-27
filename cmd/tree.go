package cmd

import (
	"github.com/spf13/cobra"

	"github.com/hasty-ai/cli/cmd/dataset"
	"github.com/hasty-ai/cli/cmd/images"
)

// BuildCmdTree defines root command and includes into it all subcommands
func BuildCmdTree() *cobra.Command {
	var c = &cobra.Command{
		Use:   "hasty",
		Short: "Hasty command line interface",
		Long: `Hasty CLI allows to connect to Hasty application and perform different actions
in a console manner.

It uses Hasty public API (see more at https://docs.hasty.ai) and needs an API
key that can be obtained using the application: https://app.hasty.ai
`,
	}

	c.AddCommand(images.BuildCmdTree())
	c.AddCommand(dataset.BuildCmdTree())

	return c
}
