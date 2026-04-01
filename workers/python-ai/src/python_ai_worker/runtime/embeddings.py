from __future__ import annotations

from typing import Any

from ..config import load_config
from ..openai_client import OpenAIEmbeddingClient, OpenAIEmbeddingConfig
from .constants import DEFAULT_EMBEDDING_MODEL, DEFAULT_LOCAL_EMBEDDING_MODEL, TOKEN_OVERLAP_EMBEDDING_MODEL

try:
    from fastembed import TextEmbedding
    from fastembed.common.model_description import ModelSource, PoolingType
except ImportError:  # pragma: no cover - optional local backend
    TextEmbedding = None
    ModelSource = None
    PoolingType = None

_FASTEMBED_CUSTOM_MODEL_DIMS = {
    "intfloat/multilingual-e5-small": 384,
    "intfloat/multilingual-e5-base": 768,
    "intfloat/multilingual-e5-large": 1024,
}
_FASTEMBED_MODEL_CACHE: dict[str, Any] = {}


def _looks_dense_embedding_model(model: str) -> bool:
    normalized = str(model or "").strip()
    return normalized != "" and normalized != TOKEN_OVERLAP_EMBEDDING_MODEL


def _is_openai_embedding_model(model: str) -> bool:
    return str(model or "").strip().startswith("text-embedding-")


def _generate_dense_embeddings(
    texts: list[str],
    *,
    model: str = "",
    dimensions: int = 0,
) -> dict[str, Any] | None:
    resolved_model = str(model or DEFAULT_EMBEDDING_MODEL).strip()
    if not _looks_dense_embedding_model(resolved_model):
        return None
    if _is_openai_embedding_model(resolved_model):
        return _generate_openai_embeddings(texts, model=resolved_model, dimensions=dimensions)
    return _generate_local_embeddings(texts, model=resolved_model, task_type="passage")


def _generate_openai_embeddings(
    texts: list[str],
    *,
    model: str = "",
    dimensions: int = 0,
) -> dict[str, Any] | None:
    resolved_model = str(model or DEFAULT_EMBEDDING_MODEL).strip()
    client = _openai_embedding_client(model_override=resolved_model, dimensions_override=dimensions)
    if client is None or not client.is_enabled():
        return None

    config = load_config()
    batch_size = max(1, config.openai_embedding_batch_size)
    vectors: list[list[float]] = []
    usage_prompt_tokens = 0
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        if not batch:
            continue
        response = client.create_embeddings(
            inputs=batch,
            model_override=resolved_model,
            dimensions_override=dimensions or None,
        )
        vectors.extend([list(vector) for vector in response["embeddings"]])
        usage = response.get("usage") or {}
        usage_prompt_tokens += int(usage.get("prompt_tokens") or 0)

    if not vectors:
        return None
    estimated_cost = _estimate_openai_embedding_cost(usage_prompt_tokens)
    return {
        "provider": "openai",
        "model": resolved_model,
        "dimensions": len(vectors[0]),
        "embeddings": vectors,
        "usage_prompt_tokens": usage_prompt_tokens,
        "usage": {
            "provider": "openai",
            "model": resolved_model,
            "operation": "embedding",
            "request_count": max(1, (len(texts) + batch_size - 1) // batch_size),
            "prompt_tokens": usage_prompt_tokens,
            "input_text_count": len(texts),
            "vector_count": len(vectors),
            "cost_estimation_status": "configured" if estimated_cost is not None else "not_configured",
            **({"estimated_cost_usd": estimated_cost} if estimated_cost is not None else {}),
        },
    }


def _generate_local_embeddings(
    texts: list[str],
    *,
    model: str = "",
    task_type: str,
) -> dict[str, Any] | None:
    resolved_model = str(model or load_config().local_embedding_model or DEFAULT_LOCAL_EMBEDDING_MODEL).strip()
    if not resolved_model or TextEmbedding is None:
        return None
    embedding_model = _local_embedding_model(resolved_model)
    if embedding_model is None:
        return None
    prepared_inputs = _prepare_local_embedding_inputs(texts, model=resolved_model, task_type=task_type)
    try:
        embeddings = [list(vector.tolist()) for vector in embedding_model.embed(prepared_inputs)]
    except Exception:
        return None
    if not embeddings:
        return None
    return {
        "provider": "fastembed",
        "model": resolved_model,
        "dimensions": len(embeddings[0]),
        "embeddings": embeddings,
        "usage_prompt_tokens": 0,
        "usage": {
            "provider": "fastembed",
            "model": resolved_model,
            "operation": "embedding",
            "request_count": 1,
            "input_text_count": len(texts),
            "vector_count": len(embeddings),
            "cost_estimation_status": "free_local",
            "estimated_cost_usd": 0.0,
        },
    }


def _generate_query_embedding(
    text: str,
    *,
    model: str = "",
    dimensions: int = 0,
) -> list[float] | None:
    if not str(text or "").strip():
        return None
    resolved_model = str(model or DEFAULT_EMBEDDING_MODEL).strip()
    if _is_openai_embedding_model(resolved_model):
        response = _generate_openai_embeddings([text], model=resolved_model, dimensions=dimensions)
    else:
        response = _generate_local_embeddings([text], model=resolved_model, task_type="query")
    if response is None:
        return None
    embeddings = list(response.get("embeddings") or [])
    if not embeddings:
        return None
    return list(embeddings[0])


def _openai_embedding_client(
    *,
    model_override: str = "",
    dimensions_override: int = 0,
) -> OpenAIEmbeddingClient | None:
    config = load_config()
    model = str(model_override or config.openai_embedding_model or DEFAULT_EMBEDDING_MODEL).strip()
    if not _looks_dense_embedding_model(model):
        return None
    dimensions = dimensions_override if dimensions_override > 0 else config.openai_embedding_dimensions
    return OpenAIEmbeddingClient(
        OpenAIEmbeddingConfig(
            api_key=config.openai_api_key,
            model=model,
            api_url=config.openai_api_url,
            timeout_sec=config.openai_timeout_sec,
            dimensions=dimensions if dimensions > 0 else None,
        )
    )


def _local_embedding_model(model: str) -> Any | None:
    resolved_model = str(model or "").strip()
    if not resolved_model or TextEmbedding is None:
        return None
    cached = _FASTEMBED_MODEL_CACHE.get(resolved_model)
    if cached is not None:
        return cached
    _register_fastembed_custom_model(resolved_model)
    try:
        embedding_model = TextEmbedding(model_name=resolved_model)
    except Exception:
        return None
    _FASTEMBED_MODEL_CACHE[resolved_model] = embedding_model
    return embedding_model


def _register_fastembed_custom_model(model: str) -> None:
    if TextEmbedding is None or ModelSource is None or PoolingType is None:
        return
    dim = _FASTEMBED_CUSTOM_MODEL_DIMS.get(str(model or "").strip())
    if dim is None:
        return
    try:
        supported = {str(item.get("model") or "").strip() for item in TextEmbedding.list_supported_models()}
    except Exception:
        supported = set()
    if model in supported:
        return
    try:
        TextEmbedding.add_custom_model(
            model=model,
            pooling=PoolingType.MEAN,
            normalization=True,
            sources=ModelSource(hf=model),
            dim=dim,
            model_file="onnx/model.onnx",
        )
    except Exception:
        return


def _prepare_local_embedding_inputs(texts: list[str], *, model: str, task_type: str) -> list[str]:
    if "e5" not in str(model or "").lower():
        return [str(text or "") for text in texts]
    prefix = "query: " if task_type == "query" else "passage: "
    return [prefix + str(text or "") for text in texts]


def _estimate_openai_embedding_cost(prompt_tokens: int) -> float | None:
    config = load_config()
    price_per_million = max(0.0, float(config.openai_embedding_price_per_million_tokens))
    if price_per_million <= 0.0:
        return None
    return round((float(max(0, prompt_tokens)) * price_per_million) / 1_000_000.0, 8)


__all__ = [
    "_generate_dense_embeddings",
    "_generate_local_embeddings",
    "_generate_openai_embeddings",
    "_generate_query_embedding",
    "_is_openai_embedding_model",
    "_looks_dense_embedding_model",
]
