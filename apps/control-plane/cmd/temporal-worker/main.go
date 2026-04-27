package main

import (
	"os"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/obs"
	"analysis-support-platform/control-plane/internal/service"
	"analysis-support-platform/control-plane/internal/skills"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

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
	if cfg.TemporalPersistenceMode == "dev_ephemeral" || cfg.TemporalRetentionMode == "temporal_dev_default" {
		obs.Logger.Warn("temporal history durability is limited; worker relies on startup reconciliation and persisted app metadata for recovery")
	}

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
	analysisService := service.NewAnalysisService(repository, starter, nil)
	analysisService.SetDependencyBuilder(datasetService)
	answerGenerator := skills.PythonAIFinalAnswerClient{BaseURL: cfg.PythonAIWorkerURL}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		obs.Logger.Error("failed to connect to temporal", "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	registerAnalysisWorker := func(w worker.Worker) {
		stepHooks := []skills.StepHook{
			skills.RuntimeStepHook{},
			skills.ExecutionProgressHook{Repo: repository},
		}
		workflows.RegisterAnalysisRuntime(w, workflows.AnalysisActivities{
			Repo: repository,
			Runner: skills.CompositeRunner{
				Structured: skills.DuckDBRunner{Path: cfg.DuckDBPath, Hooks: stepHooks},
				Unstructured: skills.PythonAIClient{
					BaseURL:      cfg.PythonAIWorkerURL,
					ArtifactRoot: cfg.ArtifactRoot,
					Hooks:        stepHooks,
				},
			},
			AnswerGenerator: answerGenerator,
			Now:             workflows.NewAnalysisActivities().Now,
		})
	}
	registerBuildWorker := func(w worker.Worker) {
		workflows.RegisterDatasetBuildRuntime(w, &workflows.DatasetBuildActivities{
			Repo:    repository,
			Builder: datasetService,
			Resumer: analysisService,
			Now:     workflows.NewAnalysisActivities().Now,
			Concurrency: workflows.DatasetBuildConcurrencyLimits{
				Prepare:   cfg.DatasetBuildPrepareMaxConcurrent,
				Sentiment: cfg.DatasetBuildSentimentMaxConcurrent,
				Embedding: cfg.DatasetBuildEmbeddingMaxConcurrent,
				Cluster:   cfg.DatasetBuildClusterMaxConcurrent,
			},
		})
	}

	if cfg.TemporalTaskQueue == cfg.TemporalBuildTaskQueue {
		maxConcurrentActivities := cfg.TemporalAnalysisMaxConcurrentActivities
		if cfg.TemporalBuildMaxConcurrentActivities > maxConcurrentActivities {
			maxConcurrentActivities = cfg.TemporalBuildMaxConcurrentActivities
		}
		w := worker.New(temporalClient, cfg.TemporalTaskQueue, worker.Options{
			MaxConcurrentActivityExecutionSize: maxConcurrentActivities,
		})
		registerAnalysisWorker(w)
		registerBuildWorker(w)
		obs.Logger.Info("worker registered",
			"event", "temporal.worker.lifecycle",
			"phase", "register",
			"queue", cfg.TemporalTaskQueue,
			"type", "analysis+build",
			"address", cfg.TemporalAddress,
			"namespace", cfg.TemporalNamespace,
			"analysis_max", cfg.TemporalAnalysisMaxConcurrentActivities,
			"build_max", cfg.TemporalBuildMaxConcurrentActivities,
			"prepare_max", cfg.DatasetBuildPrepareMaxConcurrent,
			"sentiment_max", cfg.DatasetBuildSentimentMaxConcurrent,
			"embedding_max", cfg.DatasetBuildEmbeddingMaxConcurrent,
			"cluster_max", cfg.DatasetBuildClusterMaxConcurrent,
		)
		if err := w.Run(worker.InterruptCh()); err != nil {
			obs.Logger.Error("worker exited with error", "error", err)
			os.Exit(1)
		}
		return
	}

	analysisWorker := worker.New(temporalClient, cfg.TemporalTaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.TemporalAnalysisMaxConcurrentActivities,
	})
	buildWorker := worker.New(temporalClient, cfg.TemporalBuildTaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.TemporalBuildMaxConcurrentActivities,
	})
	registerAnalysisWorker(analysisWorker)
	registerBuildWorker(buildWorker)

	if err := analysisWorker.Start(); err != nil {
		obs.Logger.Error("failed to start analysis worker", "error", err)
		os.Exit(1)
	}
	if err := buildWorker.Start(); err != nil {
		analysisWorker.Stop()
		obs.Logger.Error("failed to start build worker", "error", err)
		os.Exit(1)
	}

	obs.Logger.Info("workers registered",
		"event", "temporal.worker.lifecycle",
		"phase", "register",
		"type", "analysis+build",
		"address", cfg.TemporalAddress,
		"namespace", cfg.TemporalNamespace,
		"analysis_queue", cfg.TemporalTaskQueue,
		"build_queue", cfg.TemporalBuildTaskQueue,
		"analysis_max", cfg.TemporalAnalysisMaxConcurrentActivities,
		"build_max", cfg.TemporalBuildMaxConcurrentActivities,
		"prepare_max", cfg.DatasetBuildPrepareMaxConcurrent,
		"sentiment_max", cfg.DatasetBuildSentimentMaxConcurrent,
		"embedding_max", cfg.DatasetBuildEmbeddingMaxConcurrent,
		"cluster_max", cfg.DatasetBuildClusterMaxConcurrent,
	)

	<-worker.InterruptCh()
	obs.Logger.Info("shutdown signal received", "event", "service.shutdown")
	buildWorker.Stop()
	analysisWorker.Stop()
}
