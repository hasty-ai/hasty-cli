package hasty

import "github.com/hasty-ai/hasty-go"

// Config configures client
type Config struct {
	Key string `envconfig:"HASTY_API_KEY" required:"true"`
}

// New instantiates the client
func New(conf Config) *Client {
	return &Client{
		cli: hasty.NewClient(conf.Key),
	}
}

// Client can import images into hasty by their URL
type Client struct {
	cli *hasty.Client
}
