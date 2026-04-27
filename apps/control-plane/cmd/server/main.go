package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	apphttp "analysis-support-platform/control-plane/internal/http"
	"analysis-support-platform/control-plane/internal/obs"
)

func main() {
	obs.Init("control-plane")
	obs.Logger.Info("starting", "event", "service.boot.started")

	displaytime.UseKSTAsLocal()
	cfg := config.Load()

	obs.Logger.Info("configuration loaded",
		"event", "config.loaded",
		"workflow_engine", cfg.WorkflowEngine,
		"store_backend", cfg.StoreBackend,
		"planner_backend", cfg.PlannerBackend,
		"bind_addr", cfg.BindAddr,
	)

	if cfg.WorkflowEngine == "temporal" {
		obs.Logger.Info("temporal runtime configured",
			"persistence", cfg.TemporalPersistenceMode,
			"retention", cfg.TemporalRetentionMode,
			"recovery", cfg.TemporalRecoveryMode,
			"address", cfg.TemporalAddress,
			"namespace", cfg.TemporalNamespace,
		)
		if cfg.TemporalPersistenceMode == "dev_ephemeral" || cfg.TemporalRetentionMode == "temporal_dev_default" {
			obs.Logger.Warn("temporal history durability is limited; relying on startup reconciliation and persisted state for recovery")
		}
	}

	server := apphttp.NewServer(cfg)
	if err := server.RunStartupReconciliation(); err != nil {
		obs.Logger.Error("startup reconciliation failed", "error", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		obs.Logger.Info("shutdown signal received", "event", "service.shutdown")
	}()

	obs.Logger.Info("listening", "event", "service.boot.completed", "addr", cfg.BindAddr)
	if err := http.ListenAndServe(cfg.BindAddr, server.Handler()); err != nil {
		obs.Logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
