package main

import (
	"log"
	"net/http"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	apphttp "analysis-support-platform/control-plane/internal/http"
)

func main() {
	displaytime.UseKSTAsLocal()
	cfg := config.Load()
	if cfg.WorkflowEngine == "temporal" {
		log.Printf(
			"temporal runtime mode: persistence=%s retention=%s recovery=%s address=%s namespace=%s",
			cfg.TemporalPersistenceMode,
			cfg.TemporalRetentionMode,
			cfg.TemporalRecoveryMode,
			cfg.TemporalAddress,
			cfg.TemporalNamespace,
		)
		if cfg.TemporalPersistenceMode == "dev_ephemeral" || cfg.TemporalRetentionMode == "temporal_dev_default" {
			log.Printf("warning: Temporal history durability is limited in the current runtime; rely on startup reconciliation plus Postgres/artifact state for recovery")
		}
	}
	server := apphttp.NewServer(cfg)
	if err := server.RunStartupReconciliation(); err != nil {
		log.Printf("startup reconciliation failed: %v", err)
	}

	log.Printf("control-plane listening on %s", cfg.BindAddr)
	if err := http.ListenAndServe(cfg.BindAddr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
