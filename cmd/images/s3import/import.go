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

	"github.com/hasty-ai/hasty-go"
)

const batchSize int64 = 100
const region = "eu-central-1" // Germany, closest to Hasty, but it does not really matter
const signTimeout = 1 * time.Hour
const inFlight = 10 // Buffer size
const httpTimeout = 5 * time.Second
const maxImportErrors = 10 // Max consequent import errors before failing whole run

type config struct {
	Bucket    string
	Prefix    string
	Project   string
	Dataset   string
	AWSKey    string `envconfig:"AWS_ACCESS_KEY_ID" required:"true"`
	AWSSecret string `envconfig:"AWS_SECRET_ACCESS_KEY" required:"true"`
	HastyKey  string `envconfig:"HASTY_API_KEY" required:"true"`
}

type image struct {
	URL      string
	Path     string
	Filename string
}

type importer struct {
	config config
}

func (i *importer) run(cmd *cobra.Command, args []string) {
	if err := envconfig.Process("", &i.config); err != nil {
		log.Errorf("Unable to read configuration from environment variables: %s", err)
		return
	}

	log.Info("Perform images import from AWS S3")
	// This context will be cancelled when all images are imported, or on problem
	ctx, cancel := context.WithCancel(cmd.Context())

	ch := make(chan image, inFlight)

	go i.fetchFromS3(ctx, ch)
	go i.importToHasty(ctx, cancel, ch)

	<-ctx.Done()
}

func (i *importer) fetchFromS3(ctx context.Context, ch chan<- image) {
	// Irrelevant of what happens, the channel must be closed, as another goroutine waits for that
	defer close(ch)

	log.Debug("Configure AWS client")
	cfg := &aws.Config{
		Region: aws.String(region),
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		log.Errorf("Unable to instantiate AWS API session: %s", err)
		return
	}
	svc := s3.New(sess)

	log.Debug("Find out S3 bucket location region")
	resp, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: &i.config.Bucket,
	})
	if err != nil {
		log.Errorf("Unable to get S3 bucket location: %s", err)
		return
	}

	log.Debug("Re-instantiate AWS client with proper region in config")
	cfg.Region = resp.LocationConstraint
	sess, err = session.NewSession(cfg)
	if err != nil {
		log.Errorf("Unable to instantiate AWS API session: %s", err)
		return
	}
	svc = s3.New(sess)

	log.Debug("Listing S3 objects in bucket")
	errs := 0
	f := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, o := range page.Contents {
			if errs > maxImportErrors {
				log.Error("Too many errors happened one by one, giving up import")
				return false // Proceed to completion of import
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
			ch <- image{
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

func (i *importer) importToHasty(ctx context.Context, cancel context.CancelFunc, ch <-chan image) {
	// Irrelevant of what happens, context has to be cancelled, as main goroutine waits for that
	defer cancel()

	log.Debug("Instantiate Hasty client")
	hc := hasty.NewClient(i.config.HastyKey)

	errs := 0
	for img := range ch {
		if errs > maxImportErrors {
			log.Error("Too many errors happened one by one, giving up import")
			return // Proceed to completion of import
		}
		callCtx, callCancel := context.WithTimeout(ctx, httpTimeout)
		params := &hasty.ImageUploadExternalParams{
			Project:  &i.config.Project,
			Dataset:  &i.config.Dataset,
			URL:      &img.URL,
			Copy:     hasty.Bool(true),
			Filename: &img.Filename,
		}
		if _, err := hc.Image.UploadExternal(callCtx, params); err == nil {
			errs = 0 // Reset errors counter
		} else {
			log.Warnf("Unable to import file %s: %s", img.Path, err)
			errs++
		}
		callCancel() // Avoid contexts leak
	}
	// Channel is closed by another goroutine
	log.Info("Import is done")
}
