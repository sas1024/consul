package testutil

import (
	consulapi "github.com/hashicorp/consul/api"
	"github.com/l-vitaly/consul"
)

func defaultServerConfig() *consulapi.Config {
	return consulapi.DefaultConfig()
}

func NewClient() (consul.Client, error) {
	c, err := consulapi.NewClient(defaultServerConfig())
	if err != nil {
		return nil, err
	}
	return consul.NewClientWithConsulClient(c), nil
}
