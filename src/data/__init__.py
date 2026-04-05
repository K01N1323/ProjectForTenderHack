from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "CONTRACT_DATASET_CANDIDATES": (".personalization_data", "CONTRACT_DATASET_CANDIDATES"),
    "REQUIRED_CONTRACT_COLUMNS": (".personalization_data", "REQUIRED_CONTRACT_COLUMNS"),
    "REQUIRED_STE_COLUMNS": (".personalization_data", "REQUIRED_STE_COLUMNS"),
    "STE_DATASET_CANDIDATES": (".personalization_data", "STE_DATASET_CANDIDATES"),
    "ContractRecord": (".personalization_data", "ContractRecord"),
    "DatasetPaths": (".personalization_data", "DatasetPaths"),
    "LoadedDatasets": (".personalization_data", "LoadedDatasets"),
    "STERecord": (".personalization_data", "STERecord"),
    "load_and_validate_datasets": (".personalization_data", "load_and_validate_datasets"),
    "resolve_dataset_paths": (".personalization_data", "resolve_dataset_paths"),
    "write_data_contract_report": (".personalization_data", "write_data_contract_report"),
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
