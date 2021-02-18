package s3import

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	client "github.com/hasty-ai/cli/lib/hasty"
)

const batchSize int64 = 100
const region = "eu-central-1" // Germany, closest to Hasty, but it does not really matter
const signTimeout = 1 * time.Hour
const inFlight = 10        // Buffer size
const maxImportErrors = 10 // Max consequent import errors before failing whole run

type config struct {
	client.Config
	Bucket    string
	Prefix    string
	Project   string
	Dataset   string
	AWSKey    string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecret string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`
}

type importer struct {
	config config
}

func (i *importer) run(cmd *cobra.Command, args []string) {
	if err := envconfig.Process("", &i.config); err != nil {
		log.Fatalf("Unable to read configuration from environment variables: %s", err)
	}

	log.Info("Perform images import from AWS S3")
	// This context will be cancelled when all images are imported, or on problem
	ctx, cancel := context.WithCancel(cmd.Context())

	hc := client.New(i.config.Config)
	ch := make(chan client.Image, inFlight)

	go i.fetch(ctx, ch)
	go func() {
		hc.ImportImages(ctx, i.config.Project, i.config.Dataset, ch)
		cancel()
	}()

	<-ctx.Done()
}

func (i *importer) fetch(ctx context.Context, ch chan<- client.Image) {
	// Irrelevant of what happens, the channel must be closed, as another goroutine waits for that
	defer close(ch)

	log.Debug("Configure AWS client")
	cfg := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		log.Fatalf("Unable to instantiate AWS API session: %s", err)
	}
	svc := s3.New(sess)

	log.Debug("Find out S3 bucket location region")
	resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: &i.config.Bucket,
	})
	if err != nil {
		log.Fatalf("Unable to get S3 bucket location: %s", err)
	}

	log.Debug("Re-instantiate AWS client with proper region in config")
	cfg.Region = resp.LocationConstraint
	sess, err = session.NewSession(cfg)
	if err != nil {
		log.Fatalf("Unable to instantiate AWS API session: %s", err)
	}
	svc = s3.New(sess)

	log.Debug("Listing S3 objects in bucket")
	errs := 0
	f := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, o := range page.Contents {
			if errs > maxImportErrors {
				log.Fatalf("Too many errors happened one by one, giving up import")
			}
			if strings.HasSuffix(*o.Key, "/") {
				log.Infof("Skip directory '%s'", *o.Key)
				continue
			}
			log.Infof("Process '%s'", *o.Key)
			req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
				Bucket: &i.config.Bucket,
				Key:    o.Key,
			})
			link, err := req.Presign(signTimeout)
			if err == nil {
				errs = 0 // Reset errors counter
			} else {
				log.Warnf("Unable to get signed link for file %s: %s", *o.Key, err)
				errs++
				break
			}
			// Send the image to importer
			ch <- client.Image{
				URL:      link,
				Path:     *o.Key,
				Filename: filepath.Base(*o.Key),
			}
		}
		return true
	}
	listParams := &s3.ListObjectsV2Input{
		Bucket:  &i.config.Bucket,
		MaxKeys: aws.Int64(batchSize),
	}
	if i.config.Prefix != "" {
		listParams.Prefix = &i.config.Prefix
	}
	if err = svc.ListObjectsV2Pages(listParams, f); err != nil {
		log.Errorf("Unable to list S3 bucket: %s", err)
		return
	}
}
