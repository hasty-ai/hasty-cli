package images

import (
	"github.com/spf13/cobra"

	"github.com/hasty-ai/cli/cmd/images/gcsimport"
	"github.com/hasty-ai/cli/cmd/images/s3import"
)

// BuildCmdTree defines images command and includes into it all subcommands
func BuildCmdTree() *cobra.Command {
	var c = &cobra.Command{
		Use:   "images",
		Short: "Bulk images manipulations",
	}

	c.AddCommand(s3import.BuildCmdTree())
	c.AddCommand(gcsimport.BuildCmdTree())

	return c
}
