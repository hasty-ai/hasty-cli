package create

import (
	"github.com/spf13/cobra"
)

// BuildCmdTree defines create command
func BuildCmdTree() *cobra.Command {
	command := &cmd{}

	var c = &cobra.Command{
		Use:   "create",
		Short: "Create dataset",
		Long: `Create dataset

	This command creates dataset and returns its details in JSON format.

	Command requires the following environment variables set to respective values:
	- HASTY_API_KEY
	`,
		Run: command.run,
	}

	c.Flags().StringVar(&command.config.Project, "project", "", "existing project ID to create dataset in (required)")
	c.Flags().StringVar(&command.config.Name, "name", "", "dataset name (required)")
	// It is fine to ignore these errors
	_ = c.MarkFlagRequired("project")
	_ = c.MarkFlagRequired("name")

	return c
}
