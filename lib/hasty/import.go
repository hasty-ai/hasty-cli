package hasty

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/hasty-ai/hasty-go"
)

const importTimeout = 5 * time.Second
const importMaxErrors = 10 // Max consequent import errors before failing whole run

// Image that can be imported
type Image struct {
	URL      string
	Path     string
	Filename string
}

// ImportImages performs images import. It starts to consume images from ch until it's closed. It can tolerate
// several errors, but once their amount reaches certain threshold, it breaks the import
func (c *Client) ImportImages(ctx context.Context, project, dataset string, ch <-chan Image) {
	errs := 0
	for img := range ch {
		if errs > importMaxErrors {
			log.Fatalf("Too many errors happened one by one, giving up import")
		}
		callCtx, cancel := context.WithTimeout(ctx, importTimeout)
		params := &hasty.ImageUploadExternalParams{
			Project:  &project,
			Dataset:  &dataset,
			URL:      &img.URL,
			Copy:     hasty.Bool(true),
			Filename: &img.Filename,
		}
		if _, err := c.cli.Image.UploadExternal(callCtx, params); err == nil {
			errs = 0 // Reset errors counter
		} else {
			log.Warnf("Unable to import file %s: %s", img.Path, err)
			errs++
		}
		cancel() // Avoid contexts leak
	}
	// Channel is closed by another goroutine
	log.Info("Import is done")
}
