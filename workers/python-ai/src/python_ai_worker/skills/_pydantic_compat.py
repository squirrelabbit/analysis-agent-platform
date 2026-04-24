from __future__ import annotations

import copy
import types
from typing import Any, get_args, get_origin, get_type_hints

try:  # pragma: no cover - exercised when real pydantic is installed
    from pydantic import BaseModel, Field, ValidationError  # type: ignore
except ImportError:  # pragma: no cover - local fallback for frozen dependency set
    _MISSING = object()

    class ValidationError(ValueError):
        """Minimal validation error compatible with pydantic-style usage."""

    class _FieldInfo:
        def __init__(self, default: Any = _MISSING, default_factory: Any | None = None) -> None:
            self.default = default
            self.default_factory = default_factory

    def Field(default: Any = _MISSING, *, default_factory: Any | None = None) -> Any:
        return _FieldInfo(default=default, default_factory=default_factory)

    class BaseModel:
        """Small subset of the pydantic BaseModel API used in this repo."""

        def __init__(self, **kwargs: Any) -> None:
            hints = get_type_hints(self.__class__)
            for field_name in hints:
                if field_name not in kwargs:
                    raise ValidationError(f"{self.__class__.__name__}.{field_name} is required")
                setattr(self, field_name, kwargs[field_name])

        @classmethod
        def model_validate(cls, value: Any) -> "BaseModel":
            if isinstance(value, cls):
                return value
            if not isinstance(value, dict):
                raise ValidationError(f"{cls.__name__} expects an object")

            hints = get_type_hints(cls)
            kwargs: dict[str, Any] = {}
            for field_name, annotation in hints.items():
                if field_name in value:
                    raw_value = value[field_name]
                else:
                    raw_value = _resolve_default(cls, field_name)
                kwargs[field_name] = _validate_value(
                    raw_value,
                    annotation,
                    field_path=f"{cls.__name__}.{field_name}",
                )
            return cls(**kwargs)

        def model_dump(self) -> dict[str, Any]:
            hints = get_type_hints(self.__class__)
            return {
                field_name: _dump_value(getattr(self, field_name))
                for field_name in hints
            }


    def _resolve_default(model_cls: type[BaseModel], field_name: str) -> Any:
        raw_default = getattr(model_cls, field_name, _MISSING)
        if isinstance(raw_default, _FieldInfo):
            if raw_default.default_factory is not None:
                return raw_default.default_factory()
            if raw_default.default is not _MISSING:
                return copy.deepcopy(raw_default.default)
            raise ValidationError(f"{model_cls.__name__}.{field_name} is required")
        if raw_default is _MISSING:
            raise ValidationError(f"{model_cls.__name__}.{field_name} is required")
        return copy.deepcopy(raw_default)


    def _validate_value(value: Any, annotation: Any, *, field_path: str) -> Any:
        origin = get_origin(annotation)
        args = get_args(annotation)

        if annotation is Any:
            return value
        if annotation is None or annotation is type(None):
            if value is None:
                return None
            raise ValidationError(f"{field_path} must be null")
        if origin in {types.UnionType, getattr(__import__("typing"), "Union", object())}:
            last_error: Exception | None = None
            for option in args:
                try:
                    return _validate_value(value, option, field_path=field_path)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
            raise ValidationError(str(last_error or f"{field_path} does not match any allowed type"))
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return annotation.model_validate(value)
        if origin is list:
            if not isinstance(value, list):
                raise ValidationError(f"{field_path} must be a list")
            item_type = args[0] if args else Any
            return [
                _validate_value(item, item_type, field_path=f"{field_path}[{index}]")
                for index, item in enumerate(value)
            ]
        if origin is dict:
            if not isinstance(value, dict):
                raise ValidationError(f"{field_path} must be an object")
            key_type = args[0] if args else Any
            value_type = args[1] if len(args) > 1 else Any
            normalized: dict[Any, Any] = {}
            for raw_key, raw_value in value.items():
                key = _validate_value(raw_key, key_type, field_path=f"{field_path}.<key>")
                normalized[key] = _validate_value(raw_value, value_type, field_path=f"{field_path}.{key}")
            return normalized
        if annotation is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
                return value.strip().lower() == "true"
            raise ValidationError(f"{field_path} must be a boolean")
        if annotation is int:
            if isinstance(value, bool):
                raise ValidationError(f"{field_path} must be an integer")
            try:
                return int(value)
            except (TypeError, ValueError) as exc:
                raise ValidationError(f"{field_path} must be an integer") from exc
        if annotation is float:
            try:
                return float(value)
            except (TypeError, ValueError) as exc:
                raise ValidationError(f"{field_path} must be a number") from exc
        if annotation is str:
            if value is None:
                raise ValidationError(f"{field_path} must be a string")
            return str(value)
        return value


    def _dump_value(value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump()
        if isinstance(value, list):
            return [_dump_value(item) for item in value]
        if isinstance(value, dict):
            return {key: _dump_value(item) for key, item in value.items()}
        return value


__all__ = [
    "BaseModel",
    "Field",
    "ValidationError",
]
