package dataset

import (
	"github.com/spf13/cobra"

	"github.com/hasty-ai/cli/cmd/dataset/create"
)

// BuildCmdTree defines dataset command and includes into it all subcommands
func BuildCmdTree() *cobra.Command {
	var c = &cobra.Command{
		Use:   "dataset",
		Short: "Dataset manipulations",
	}

	c.AddCommand(create.BuildCmdTree())

	return c
}
