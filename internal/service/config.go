package service

import (
	"fmt"
	"log"

	"PushOccurrence/config"
)

func LoadConfig(path string) *config.Config {
	cfg, err := config.Load("config/config.json")
	if err != nil {
		log.Fatalf("can't load config: %v", err)
	}

	return cfg
}

func BuildConnString(cfg *config.Config) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", cfg.Postgres.User, cfg.Postgres.Password, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database, cfg.Postgres.SSLMode)
}
