from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "EXPLAIN_RULES": (".personalization_features", "EXPLAIN_RULES"),
    "FEATURE_DEFAULTS": (".personalization_features", "FEATURE_DEFAULTS"),
    "FEATURE_SPEC": (".personalization_features", "FEATURE_SPEC"),
    "GlobalHistoryState": (".personalization_features", "GlobalHistoryState"),
    "UserHistoryState": (".personalization_features", "UserHistoryState"),
    "build_feature_vector": (".personalization_features", "build_feature_vector"),
    "build_inference_feature_vector": (".personalization_features", "build_inference_feature_vector"),
    "build_query_context": (".personalization_features", "build_query_context"),
    "build_reason_trace": (".personalization_features", "build_reason_trace"),
    "derive_item_kind": (".personalization_features", "derive_item_kind"),
    "generate_pseudo_queries": (".personalization_features", "generate_pseudo_queries"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
