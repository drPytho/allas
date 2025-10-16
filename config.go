package main

import (
	"fmt"
	"os"
)

type Config struct {
	BindAddr string

	DatabaseAddr string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		// Set defaults
		BindAddr:     getEnv("BIND_ADDR", ":8080"),
		DatabaseAddr: getEnv("DATABASE_URL", ""),
	}
	// Validate required fields
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.BindAddr == "" {
		return fmt.Errorf("BIND_ADDR is required")
	}
	if c.DatabaseAddr == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
