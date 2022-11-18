package gcsimport

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	client "github.com/hasty-ai/cli/lib/hasty"
)

const signTimeout = 1 * time.Hour
const inFlight = 10        // Buffer size
const maxImportErrors = 10 // Max consequent import errors before failing whole run
const scope = "https://www.googleapis.com/auth/devstorage.read_only"

type config struct {
	client.Config
	Bucket    string
	Prefix    string
	Project   string
	Dataset   string
	CredsFile string `envconfig:"GCP_KEY_PATH" required:"true"`
}

type importer struct {
	config config
}

func (i *importer) run(cmd *cobra.Command, args []string) {
	if err := envconfig.Process("", &i.config); err != nil {
		log.Fatalf("Unable to read configuration from environment variables: %s", err)
	}

	log.Info("Perform images import from GCS")
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

	log.Debug("Configure GCS client")
	keyJSON, err := os.ReadFile(i.config.CredsFile)
	if err != nil {
		log.Fatalf("Unable to read credentials from file `%s`: %s", i.config.CredsFile, err)
	}
	creds, err := google.CredentialsFromJSON(ctx, keyJSON, scope)
	if err != nil {
		log.Fatalf("Unable to retrieve credentials from provided key: %s", err)
	}
	conf, err := google.JWTConfigFromJSON(keyJSON, scope)
	if err != nil {
		log.Fatalf("Unable to get config from provided key: %s", err)
	}
	c, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		log.Fatalf("Unable to instantiate GCS client: %s", err)
	}

	log.Debug("Listing objects in bucket")
	errs := 0
	stor := c.Bucket(i.config.Bucket)
	query := &storage.Query{Prefix: i.config.Prefix}
	it := stor.Objects(ctx, query)
	for {
		if errs > maxImportErrors {
			log.Fatalf("Too many errors happened one by one, giving up import")
		}
		attrs, err := it.Next()
		if err == iterator.Done {
			return
		}
		if err != nil {
			log.Warnf("Unable to list objects: %s", err)
			errs++
			continue
		}
		if strings.HasSuffix(attrs.Name, "/") {
			log.Infof("Skip directory '%s'", attrs.Name)
			continue
		}

		opts := &storage.SignedURLOptions{
			GoogleAccessID: conf.Email,
			PrivateKey:     conf.PrivateKey,
			Method:         http.MethodGet,
			Expires:        time.Now().Add(signTimeout),
		}
		url, err := storage.SignedURL(i.config.Bucket, attrs.Name, opts)
		if err != nil {
			log.Warnf("Unable sign URL for `%s`: %s", attrs.Name, err)
			errs++
			continue
		}
		errs = 0 // Reset errors counter

		// Send the image to importer
		ch <- client.Image{
			URL:      url,
			Path:     attrs.Name,
			Filename: filepath.Base(attrs.Name),
		}
	}
}
