package consul

import (
	"errors"
	"net"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"
)

var (
	ErrInvalidServiceAddr = errors.New("invalid service address")
	ErrInvalidPort        = errors.New("invalid port")
	ErrServiceNotFound    = errors.New("service not found")
    ErrKVNotFound         = errors.New("kv not found")
)

//Client provides an interface for getting data out of Consul
type Client interface {
	// GetServices get a services from consul
	GetServices(service string, tag string) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error)
	// GetFirstService get a first service from consul
	GetFirstService(service string, tag string) (*consulapi.ServiceEntry, *consulapi.QueryMeta, error)
	// RegisterService register a service with local agent
	RegisterService(name string, addr string, tags ...string) error
	// DeRegisterService deregister a service with local agent
	DeRegisterService(string) error
	// Get get KVPair
	Get(key string) (*consulapi.KVPair, *consulapi.QueryMeta, error)
	// WatchGet
	WatchGet(key string) chan *consulapi.KVPair
	// GetStr get string value
	GetStr(key string) (string, error)
	// Put put KVPair
	Put(key string, value string) (*consulapi.WriteMeta, error)
}

type client struct {
	kv     *consulapi.KV
	health *consulapi.Health
	meta   map[string]*consulapi.QueryMeta
	agent  *consulapi.Agent
}

// NewClient returns a Client interface for given consul address
func NewClientWithConsulClient(c *consulapi.Client) Client {
	return &client{
		kv:     c.KV(),
		health: c.Health(),
		agent:  c.Agent(),
		meta:   make(map[string]*consulapi.QueryMeta),
	}
}

// NewClient returns a Client interface for given consul address
func NewClient(addr string) (Client, error) {
	config := consulapi.DefaultConfig()
	config.Address = addr
	c, err := consulapi.NewClient(config)
	if err != nil {
		return nil, err
	}
	return NewClientWithConsulClient(c), nil
}

// Get KVPair
func (c *client) Get(key string) (*consulapi.KVPair, *consulapi.QueryMeta, error) {
	kv, meta, err := c.kv.Get(key, nil)
	if err != nil {
		return nil, nil, err
	}
    if kv == nil {
        return nil, nil, ErrKVNotFound
    }

	c.meta[key] = meta

	return kv, meta, nil
}

func (c *client) WatchGet(key string) chan *consulapi.KVPair {
	doneCh := make(chan *consulapi.KVPair)
	go func(k string, ch chan *consulapi.KVPair) {
		for {
			var lastIndex uint64 = 1
			if meta, ok := c.meta[key]; ok {
				lastIndex = meta.LastIndex
			}
			kv, meta, err := c.kv.Get(k, &consulapi.QueryOptions{WaitIndex: lastIndex})

            if lastIndex == 1 && kv == nil {
                continue
            }

			if err != nil {
				close(ch)
			}
			c.meta[key] = meta
			ch <- kv
		}
	}(key, doneCh)
	return doneCh
}

// GetStr string
func (c *client) GetStr(key string) (string, error) {
	kv, _, err := c.Get(key)
	if err != nil {
		return "", err
	}
	return string(kv.Value), nil
}

// Put KVPair
func (c *client) Put(key string, value string) (*consulapi.WriteMeta, error) {
	p := &consulapi.KVPair{Key: key, Value: []byte(value)}
	return c.kv.Put(p, nil)
}

// RegisterService a service with consul local agent
func (c *client) RegisterService(name string, addr string, tags ...string) error {
	host, strPort, err := net.SplitHostPort(addr)
	if err != nil {
		return ErrInvalidServiceAddr
	}

	port, err := strconv.Atoi(strPort)
	if err != nil {
		return ErrInvalidPort
	}

	reg := &consulapi.AgentServiceRegistration{
		ID:      name,
		Name:    name,
		Address: host,
		Port:    port,
		Tags:    tags,
	}
	return c.agent.ServiceRegister(reg)
}

// DeRegisterService a service with consul local agent
func (c *client) DeRegisterService(id string) error {
	return c.agent.ServiceDeregister(id)
}

// GetFirstService get first service
func (c *client) GetFirstService(service string, tag string) (*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	addrs, meta, err := c.GetServices(service, tag)
	if err != nil {
		return nil, nil, err
	}
	if len(addrs) == 0 {
		return nil, nil, ErrServiceNotFound
	}
	return addrs[0], meta, nil
}

// GetServices return a services
func (c *client) GetServices(service string, tag string) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	passingOnly := true
	addrs, meta, err := c.health.Service(service, tag, passingOnly, nil)
	if err != nil {
		return nil, nil, err
	}
	if len(addrs) == 0 {
		return nil, nil, ErrServiceNotFound
	}
	return addrs, meta, nil
}
