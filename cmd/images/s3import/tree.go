package s3import

import (
	"github.com/spf13/cobra"
)

// BuildCmdTree defines s3-import command
func BuildCmdTree() *cobra.Command {
	i := &importer{}

	var c = &cobra.Command{
		Use:   "s3-import",
		Short: "Import from AWS S3 bucket",
		Long: `Import from AWS S3 bucket

	This command will list given S3 bucket (see --bucket flag), find files that
	start with given prefix (if provided with flag --prefix), then generate for
	them short-living signed URLs and pass these URLs to Hasty, where these files
	will be downloaded and stored in a dataset (see --dataset flag) that belongs
	to particular project (see --project flag).

	Files path prefix can be folder (then should end with slash) and/or filename
	prefix, e.g. 'folder/subfolder/' or 'folder/DCS_123'.

	Both dataset and project should be provided and must be UUIDs.

	Command requires the following environment variables set to respective values:
	- AWS_ACCESS_KEY_ID
	- AWS_SECRET_ACCESS_KEY
	- HASTY_API_KEY
	`,
		Run: i.run,
	}

	c.Flags().StringVar(&i.config.Bucket, "bucket", "", "S3 bucket name (required)")
	c.Flags().StringVar(&i.config.Prefix, "prefix", "", "files path prefix (optional)")
	c.Flags().StringVar(&i.config.Project, "project", "", "existing project ID to import images to (required)")
	c.Flags().StringVar(&i.config.Dataset, "dataset", "", "existing dataset ID to import images to (required)")
	c.MarkFlagRequired("bucket")
	c.MarkFlagRequired("project")
	c.MarkFlagRequired("dataset")

	return c
}
