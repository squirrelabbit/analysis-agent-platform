package main

import (
	"log"

	"analysis-support-platform/control-plane/internal/config"
	"analysis-support-platform/control-plane/internal/displaytime"
	"analysis-support-platform/control-plane/internal/service"
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
	starter := workflows.TemporalStarter{
		Address:               cfg.TemporalAddress,
		Namespace:             cfg.TemporalNamespace,
		TaskQueue:             cfg.TemporalTaskQueue,
		DatasetBuildTaskQueue: cfg.TemporalBuildTaskQueue,
	}
	datasetService := service.NewDatasetService(repository, cfg.PythonAIWorkerURL, cfg.UploadRoot, cfg.ArtifactRoot)
	if err := datasetService.SetDatasetProfilesPath(cfg.DatasetProfilesPath); err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
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
		workflows.RegisterDatasetBuildRuntime(w, workflows.DatasetBuildActivities{
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
		log.Printf(
			"temporal worker listening on %s namespace=%s analysis_queue=%s build_queue=%s analysis_max=%d build_max=%d prepare_max=%d sentiment_max=%d embedding_max=%d cluster_max=%d duckdb=%s python_ai=%s",
			cfg.TemporalAddress,
			cfg.TemporalNamespace,
			cfg.TemporalTaskQueue,
			cfg.TemporalBuildTaskQueue,
			cfg.TemporalAnalysisMaxConcurrentActivities,
			cfg.TemporalBuildMaxConcurrentActivities,
			cfg.DatasetBuildPrepareMaxConcurrent,
			cfg.DatasetBuildSentimentMaxConcurrent,
			cfg.DatasetBuildEmbeddingMaxConcurrent,
			cfg.DatasetBuildClusterMaxConcurrent,
			cfg.DuckDBPath,
			cfg.PythonAIWorkerURL,
		)
		if err := w.Run(worker.InterruptCh()); err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}
	if err := buildWorker.Start(); err != nil {
		analysisWorker.Stop()
		log.Fatal(err)
	}

	log.Printf(
		"temporal worker listening on %s namespace=%s analysis_queue=%s build_queue=%s analysis_max=%d build_max=%d prepare_max=%d sentiment_max=%d embedding_max=%d cluster_max=%d duckdb=%s python_ai=%s",
		cfg.TemporalAddress,
		cfg.TemporalNamespace,
		cfg.TemporalTaskQueue,
		cfg.TemporalBuildTaskQueue,
		cfg.TemporalAnalysisMaxConcurrentActivities,
		cfg.TemporalBuildMaxConcurrentActivities,
		cfg.DatasetBuildPrepareMaxConcurrent,
		cfg.DatasetBuildSentimentMaxConcurrent,
		cfg.DatasetBuildEmbeddingMaxConcurrent,
		cfg.DatasetBuildClusterMaxConcurrent,
		cfg.DuckDBPath,
		cfg.PythonAIWorkerURL,
	)
	<-worker.InterruptCh()
	buildWorker.Stop()
	analysisWorker.Stop()
}
