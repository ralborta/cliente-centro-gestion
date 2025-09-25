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

# Campos específicos por tipo de fuente
CREDITO_PATTERNS = [r"^credito$", r"^cr$", r"^deposit(o|os)$", r"^ingreso(s)?$"]
DEBITO_PATTERNS = [r"^debito$", r"^db$", r"^egreso(s)?$", r"^debito_automatico$"]
TOTAL_PATTERNS = [r"^total$", r"^importe_total$", r"^importe$", r"^monto$"]
COMPROBANTE_PATTERNS = [r"^comprobante$", r"^nro(_|)comprobante$", r"^numero$", r"^nro$", r"^factura$"]
TEXTO_PATTERNS = DESC_PATTERNS + [r"^texto$", r"^movimiento$", r"^concepto$", r"^detalle$"]


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


def detect_extracto_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = list(df.columns)
    fecha = _first_match(cols, DATE_PATTERNS)
    credito = _first_match(cols, CREDITO_PATTERNS)
    debito = _first_match(cols, DEBITO_PATTERNS)
    texto = _first_match(cols, TEXTO_PATTERNS) or _first_match(cols, DESC_PATTERNS)
    return {"fecha": fecha, "credito": credito, "debito": debito, "texto": texto}


def detect_libro_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = list(df.columns)
    fecha = _first_match(cols, DATE_PATTERNS)
    total = _first_match(cols, TOTAL_PATTERNS)
    comp = _first_match(cols, COMPROBANTE_PATTERNS)
    desc = _first_match(cols, TEXTO_PATTERNS) or _first_match(cols, DESC_PATTERNS)
    return {"fecha": fecha, "total": total, "comprobante": comp, "desc": desc}


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


def coerce_extracto(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    dfn = normalize_columns(df)
    cols = detect_extracto_columns(dfn)
    # Construir columnas estándar: fecha, texto, monto (con signo) y tipo
    if cols["fecha"]:
        dfn[cols["fecha"]] = dfn[cols["fecha"]].apply(parse_date)
    texto_col = cols["texto"] or ""
    if texto_col in dfn.columns:
        dfn[texto_col] = dfn[texto_col].astype(str).fillna("").map(lambda s: unidecode(s).strip())
    # monto y tipo
    monto_series = pd.Series([None] * len(dfn), dtype="float64")
    tipo_series = pd.Series([None] * len(dfn), dtype="object")
    if cols["credito"] and cols["credito"] in dfn.columns:
        cr = dfn[cols["credito"]].apply(parse_amount)
        sel = cr.fillna(0) != 0
        monto_series = monto_series.mask(sel, cr.abs())
        tipo_series = tipo_series.mask(sel, "Credito")
    if cols["debito"] and cols["debito"] in dfn.columns:
        db = dfn[cols["debito"]].apply(parse_amount)
        sel = db.fillna(0) != 0
        # Débito lo representamos con monto positivo pero tipo indica dirección
        monto_series = monto_series.mask(sel, db.abs())
        tipo_series = tipo_series.mask(sel, "Debito")
    dfn["monto"] = monto_series.fillna(0.0).astype(float)
    dfn["tipo"] = tipo_series.fillna("")
    if "__id__" not in dfn.columns:
        dfn.insert(0, "__id__", range(1, len(dfn) + 1))
    # Alias estándar
    dfn.rename(columns={texto_col: "texto", cols.get("fecha", "fecha"): "fecha"}, inplace=True)
    return dfn, cols


def coerce_libro(df: pd.DataFrame, origen: str) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    dfn = normalize_columns(df)
    cols = detect_libro_columns(dfn)
    if cols["fecha"]:
        dfn[cols["fecha"]] = dfn[cols["fecha"]].apply(parse_date)
    if cols["total"] and cols["total"] in dfn.columns:
        dfn["monto"] = dfn[cols["total"]].apply(parse_amount).astype(float)
    else:
        dfn["monto"] = 0.0
    if cols["comprobante"] and cols["comprobante"] in dfn.columns:
        dfn["comprobante"] = dfn[cols["comprobante"]].astype(str)
    else:
        dfn["comprobante"] = ""
    desc_col = cols.get("desc") or ""
    if desc_col in dfn.columns:
        dfn["desc"] = dfn[desc_col].astype(str).fillna("").map(lambda s: unidecode(s).strip())
    else:
        dfn["desc"] = ""
    if "__id__" not in dfn.columns:
        dfn.insert(0, "__id__", range(1, len(dfn) + 1))
    dfn["__origen__"] = origen
    # Alias estándar fecha
    if cols.get("fecha"):
        dfn.rename(columns={cols["fecha"]: "fecha"}, inplace=True)
    return dfn, cols



