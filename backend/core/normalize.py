"""
Utilidades de normalización para conciliación.

Objetivos:
- Normalizar nombres de columnas (minúsculas, sin tildes/espacios)
- Detectar columnas de fecha, monto e identificación/descripción
- Parsear fechas y montos a tipos uniformes
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import re
import pandas as pd
from unidecode import unidecode


def _slug(s: str) -> str:
    s = unidecode(str(s)).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [_slug(c) for c in normalized.columns]
    return normalized


@dataclass
class ColumnHints:
    date_col: Optional[str]
    amount_col: Optional[str]
    desc_col: Optional[str]
    id_col: Optional[str]


DATE_PATTERNS = [
    r"^fecha$",
    r"^fec(_\w+)?$",
    r"^date$",
]

AMOUNT_PATTERNS = [
    r"^importe$",
    r"^monto$",
    r"^debe$",
    r"^haber$",
    r"^importe_total$",
    r"^amount$",
    r"^total$",
]

DESC_PATTERNS = [
    r"^descripcion$",
    r"^detalle$",
    r"^concepto$",
    r"^glosa$",
    r"^observacion$",
    r"^description$",
]

ID_PATTERNS = [
    r"^id$",
    r"^nro?(_|)doc(umento)?$",
    r"^numero$",
    r"^ref(erencia)?$",
]


def _first_match(columns: list[str], patterns: list[str]) -> Optional[str]:
    for pat in patterns:
        rx = re.compile(pat)
        for c in columns:
            if rx.search(c):
                return c
    return None


def detect_columns(df: pd.DataFrame) -> ColumnHints:
    cols = list(df.columns)
    date_col = _first_match(cols, DATE_PATTERNS)
    amount_col = _first_match(cols, AMOUNT_PATTERNS)
    desc_col = _first_match(cols, DESC_PATTERNS)
    id_col = _first_match(cols, ID_PATTERNS)
    return ColumnHints(date_col=date_col, amount_col=amount_col, desc_col=desc_col, id_col=id_col)


def parse_amount(value) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None
    # Eliminar separadores de miles comunes
    s = s.replace("\u00A0", "").replace(" ", "").replace(".", "").replace(",", ".")
    # Restaurar posible decimal si había coma
    # Intentar float
    try:
        return float(s)
    except Exception:
        try:
            return float(re.sub(r"[^0-9\.-]", "", s))
        except Exception:
            return None


def parse_date(value) -> Optional[pd.Timestamp]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return pd.to_datetime(value, dayfirst=True, errors="coerce")
    except Exception:
        return None


def coerce_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, ColumnHints]:
    """
    - Normaliza nombres de columnas
    - Detecta fecha/monto/desc/id
    - Convierte columnas a tipos útiles si existen
    """
    df2 = normalize_columns(df)
    hints = detect_columns(df2)

    if hints.date_col and hints.date_col in df2.columns:
        df2[hints.date_col] = df2[hints.date_col].apply(parse_date)
    if hints.amount_col and hints.amount_col in df2.columns:
        df2[hints.amount_col] = df2[hints.amount_col].apply(parse_amount)
    # Descripción a string limpia
    if hints.desc_col and hints.desc_col in df2.columns:
        df2[hints.desc_col] = df2[hints.desc_col].astype(str).fillna("").map(lambda s: unidecode(s).strip())

    # ID estable
    if "__id__" not in df2.columns:
        df2.insert(0, "__id__", range(1, len(df2) + 1))

    return df2, hints



