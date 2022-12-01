package graphqlws

import "time"

type Config struct {
	ConnectionInitWaitTimeout time.Duration
	PingInterval              time.Duration
	PingTimeout               time.Duration
}

func GetDefaultConfig() *Config {
	return &Config{
		ConnectionInitWaitTimeout: 5 * time.Second,
		PingInterval:              10 * time.Second,
		PingTimeout:               5 * time.Second,
	}
}
