package cmd

import (
	"github.com/spf13/cobra"
)

// imagesCmd represents the images command
var imagesCmd = &cobra.Command{
	Use:   "images",
	Short: "Bulk images manipulations",
}

func init() {
	rootCmd.AddCommand(imagesCmd)
}
