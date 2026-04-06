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
	server := apphttp.NewServer(cfg)
	if err := server.RunStartupReconciliation(); err != nil {
		log.Printf("startup reconciliation failed: %v", err)
	}

	log.Printf("control-plane listening on %s", cfg.BindAddr)
	if err := http.ListenAndServe(cfg.BindAddr, server.Handler()); err != nil {
		log.Fatal(err)
	}
}
