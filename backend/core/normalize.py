"""
Utilidades de normalización (placeholder). Completar según las reglas reales
de tu conciliación cuando sea necesario.
"""

from __future__ import annotations

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(c).strip() for c in normalized.columns]
    return normalized


