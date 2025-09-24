from __future__ import annotations

from typing import Dict, Tuple, Any
import pandas as pd


def prep_extracto(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Prepara el DataFrame de extracto para conciliación.
    Implementación mínima: clona y asegura una columna id estable.
    """
    prepared = df.copy()
    if "__id__" not in prepared.columns:
        prepared.insert(0, "__id__", range(1, len(prepared) + 1))
    meta = {"source": "extracto", "rows": len(prepared)}
    return prepared, meta


def prep_libro(df: pd.DataFrame, origen: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Prepara el DataFrame de libro (Ventas/Compras).
    Implementación mínima: clona y etiqueta el origen.
    """
    prepared = df.copy()
    prepared["__origen__"] = origen
    if "__id__" not in prepared.columns:
        prepared.insert(0, "__id__", range(1, len(prepared) + 1))
    meta = {"source": origen, "rows": len(prepared)}
    return prepared, meta


def multipass_match(
    extracto: pd.DataFrame, ventas: pd.DataFrame, compras: pd.DataFrame
) -> Dict[int, Dict[str, Any]]:
    """
    Estrategia de conciliación mínima: placeholder sin emparejamiento real.
    Retorna un mapping por __id__ del extracto con datos de match (vacío).
    """
    return {}


def build_output_sheet(
    original_extracto: pd.DataFrame,
    prepared_extracto: pd.DataFrame,
    matches: Dict[int, Dict[str, Any]],
) -> pd.DataFrame:
    """
    Construye la hoja de salida. Implementación mínima: devuelve el extracto
    preparado y agrega columnas de estado vacías si faltan.
    """
    result = prepared_extracto.copy()
    if "__match__" not in result.columns:
        result["__match__"] = ""
    if "__detalle__" not in result.columns:
        result["__detalle__"] = ""
    return result


