package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hasty-ai/hasty-go"
	"github.com/hasty-ai/hasty-go/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const imagesS3ImportBatchSize int64 = 100
const imagesS3ImportAWSRegion = "eu-central-1" // Germany, closest to Hasty
const imagesS3ImportSignTimeout = 1 * time.Hour

// s3ImportCmd represents the s3Import command
var s3ImportCmd = &cobra.Command{
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
`,
	Run: imagesS3Import,
}

type imagesS3ImportFlagsSet struct {
	Bucket  string
	Prefix  string
	Project string
	Dataset string
}

var imagesS3ImportFlags imagesS3ImportFlagsSet

func init() {
	imagesCmd.AddCommand(s3ImportCmd)
	s3ImportCmd.Flags().StringVar(&imagesS3ImportFlags.Bucket, "bucket", "", "S3 bucket name (required)")
	s3ImportCmd.Flags().StringVar(&imagesS3ImportFlags.Prefix, "prefix", "", "files path prefix (optional)")
	s3ImportCmd.Flags().StringVar(&imagesS3ImportFlags.Project, "project", "", "existing project ID to import images to (required)")
	s3ImportCmd.Flags().StringVar(&imagesS3ImportFlags.Dataset, "dataset", "", "existing dataset ID to import images to (required)")
	s3ImportCmd.MarkFlagRequired("bucket")
	s3ImportCmd.MarkFlagRequired("project")
	s3ImportCmd.MarkFlagRequired("dataset")
}

func imagesS3Import(cmd *cobra.Command, args []string) {
	log.Info("Perform images import from AWS S3")

	log.Debug("Configure AWS client")
	cfg := &aws.Config{
		Region: aws.String(imagesS3ImportAWSRegion),
	}
	sess, err := session.NewSession(cfg)
	exitOnErr(err)
	svc := s3.New(sess)

	log.Debug("Find out S3 bucket location region")
	resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: &imagesS3ImportFlags.Bucket,
	})
	exitOnErr(err)

	log.Debug("Reinstantiate AWS client with proper region in config")
	cfg.Region = resp.LocationConstraint
	sess, err = session.NewSession(cfg)
	exitOnErr(err)
	svc = s3.New(sess)

	log.Debug("Instantiate Hasty client")
	hc := client.New(os.Getenv("HASTY_API_KEY"), nil)

	log.Debug("Listing S3 objects in bucket")
	f := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, i := range page.Contents {
			if strings.HasSuffix(*i.Key, "/") {
				log.Infof("Skip directory '%s'", *i.Key)
				continue
			}
			log.Infof("Process '%s'", *i.Key)
			req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
				Bucket: &imagesS3ImportFlags.Bucket,
				Key:    i.Key,
			})
			link, err := req.Presign(15 * time.Minute)
			if err != nil {
				log.Warnf("Unable to get signed link for file %s", *i.Key)
			}

			ctx := context.Background()
			params := &hasty.ImageUploadExternalParams{
				Project:  &imagesS3ImportFlags.Project,
				Dataset:  &imagesS3ImportFlags.Dataset,
				URL:      &link,
				Copy:     hasty.Bool(true),
				Filename: hasty.String(filepath.Base(*i.Key)),
			}
			_, err = hc.Image.UploadExternal(ctx, params)
			exitOnErr(err)
		}
		return true
	}
	listParams := &s3.ListObjectsV2Input{
		Bucket:  &imagesS3ImportFlags.Bucket,
		MaxKeys: aws.Int64(imagesS3ImportBatchSize),
	}
	if imagesS3ImportFlags.Prefix != "" {
		listParams.Prefix = &imagesS3ImportFlags.Prefix
	}
	err = svc.ListObjectsV2Pages(listParams, f)
	exitOnErr(err)

	log.Info("Import is done")
}
