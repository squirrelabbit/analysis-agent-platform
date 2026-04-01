package main

import (
	"log"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/skills"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	displaytime.UseKSTAsLocal()
	cfg := config.Load()
	repository, err := store.NewRepository(cfg)
	if err != nil {
		log.Fatal(err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer temporalClient.Close()

	w := worker.New(temporalClient, cfg.TemporalTaskQueue, worker.Options{})
	workflows.RegisterAnalysisRuntime(w, workflows.AnalysisActivities{
		Repo: repository,
		Runner: skills.CompositeRunner{
			Structured: skills.DuckDBRunner{Path: cfg.DuckDBPath},
			Unstructured: skills.PythonAIClient{
				BaseURL:      cfg.PythonAIWorkerURL,
				ArtifactRoot: cfg.ArtifactRoot,
			},
		},
		Now: workflows.NewAnalysisActivities().Now,
	})

	log.Printf(
		"temporal worker listening on %s namespace=%s task_queue=%s duckdb=%s python_ai=%s",
		cfg.TemporalAddress,
		cfg.TemporalNamespace,
		cfg.TemporalTaskQueue,
		cfg.DuckDBPath,
		cfg.PythonAIWorkerURL,
	)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal(err)
	}
}
