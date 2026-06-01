package main

import (
	"os"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/obs"
	"analysis-support-platform/control-plane/internal/service"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// silverone 2026-05-21 δ-2: 옛 analysis workflow + AnalysisActivities 흐름 제거.
// Temporal 워커는 이제 dataset_build (clean / doc_genuineness / clause_label)
// 만 등록한다. analyze_v2 (plan_v2 + executor_v2)는 현재 sync HTTP path로
// Python worker가 직접 처리하므로 Temporal 등록이 필요 없다.

func main() {
	obs.Init("temporal-worker")
	obs.Logger.Info("starting", "event", "service.boot.started")

	displaytime.UseKSTAsLocal()
	cfg := config.Load()

	obs.Logger.Info("temporal runtime configured",
		"persistence", cfg.TemporalPersistenceMode,
		"retention", cfg.TemporalRetentionMode,
		"recovery", cfg.TemporalRecoveryMode,
		"address", cfg.TemporalAddress,
		"namespace", cfg.TemporalNamespace,
	)

	repository, err := store.NewRepository(cfg)
	if err != nil {
		obs.Logger.Error("failed to create repository", "error", err)
		os.Exit(1)
	}

	starter := workflows.TemporalStarter{
		Address:               cfg.TemporalAddress,
		Namespace:             cfg.TemporalNamespace,
		TaskQueue:             cfg.TemporalTaskQueue,
		DatasetBuildTaskQueue: cfg.TemporalBuildTaskQueue,
	}
	datasetService := service.NewDatasetService(repository, cfg.PythonAIWorkerURL, cfg.UploadRoot, cfg.ArtifactRoot)
	if err := datasetService.SetDatasetProfilesPath(cfg.DatasetProfilesPath); err != nil {
		obs.Logger.Error("failed to load dataset profiles", "error", err)
		os.Exit(1)
	}
	datasetService.SetBuildJobStarter(starter)

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		obs.Logger.Error("failed to connect to temporal", "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	taskQueue := cfg.TemporalBuildTaskQueue
	if taskQueue == "" {
		taskQueue = cfg.TemporalTaskQueue
	}
	w := worker.New(temporalClient, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.TemporalBuildMaxConcurrentActivities,
	})
	workflows.RegisterDatasetBuildRuntime(w, &workflows.DatasetBuildActivities{
		Repo:    repository,
		Builder: datasetService,
	})

	obs.Logger.Info("worker registered",
		"event", "temporal.worker.lifecycle",
		"phase", "register",
		"queue", taskQueue,
		"type", "dataset_build",
	)
	if err := w.Run(worker.InterruptCh()); err != nil {
		obs.Logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}
