// Package redisclient owns shared direct and Sentinel connection configuration.
package redisclient

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Address                            string
	SentinelAddresses                  []string
	SentinelMaster                     string
	Username, Password                 string
	SentinelUsername, SentinelPassword string
	TLS                                *tls.Config
	DialTimeout, IOTimeout             time.Duration
	PoolSize                           int
}

// Normalize validates and owns mutable connection configuration; it does not dial.
func Normalize(options Options) (Options, error) {
	if options.DialTimeout == 0 {
		options.DialTimeout = 2 * time.Second
	}
	if options.IOTimeout == 0 {
		options.IOTimeout = 2 * time.Second
	}
	if options.PoolSize == 0 {
		options.PoolSize = 16
	}
	if options.PoolSize < 1 || options.PoolSize > 256 || options.DialTimeout <= 0 || options.IOTimeout <= 0 {
		return Options{}, errors.New("invalid Redis connection bounds")
	}
	if (options.Address != "") == (len(options.SentinelAddresses) > 0) || len(options.SentinelAddresses) > 0 && options.SentinelMaster == "" {
		return Options{}, errors.New("select one Redis primary discovery source")
	}
	options.SentinelAddresses = append([]string(nil), options.SentinelAddresses...)
	if options.TLS != nil {
		options.TLS = options.TLS.Clone()
	}
	return options, nil
}

// New creates a lazy client with retries disabled for uncertain mutations.
func New(cfg Options, poolSize int, onConnect func(context.Context, *redis.Conn) error) *redis.Client {
	if len(cfg.SentinelAddresses) > 0 {
		return redis.NewFailoverClient(&redis.FailoverOptions{MasterName: cfg.SentinelMaster, SentinelAddrs: cfg.SentinelAddresses, SentinelUsername: cfg.SentinelUsername, SentinelPassword: cfg.SentinelPassword, Username: cfg.Username, Password: cfg.Password, TLSConfig: cfg.TLS, DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.IOTimeout, WriteTimeout: cfg.IOTimeout, PoolSize: poolSize, MaxRetries: -1, ContextTimeoutEnabled: true, OnConnect: onConnect})
	}
	return redis.NewClient(&redis.Options{Addr: cfg.Address, Username: cfg.Username, Password: cfg.Password, TLSConfig: cfg.TLS, DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.IOTimeout, WriteTimeout: cfg.IOTimeout, PoolSize: poolSize, MaxRetries: -1, ContextTimeoutEnabled: true, OnConnect: onConnect})
}
