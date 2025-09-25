from __future__ import annotations

from typing import Dict, Tuple, Any, List
import pandas as pd
from rapidfuzz import fuzz

from .normalize import coerce_dataframe, ColumnHints, coerce_extracto, coerce_libro
from .ai_assist import rerank_candidates_with_ai


def prep_extracto(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    prepared, hints_map = coerce_extracto(df)
    hints = ColumnHints(date_col="fecha", amount_col="monto", desc_col="texto", id_col="__id__")
    meta = {"source": "extracto", "rows": len(prepared), "hints": hints.__dict__}
    return prepared, meta


def prep_libro(df: pd.DataFrame, origen: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    prepared, hints_map = coerce_libro(df, origen)
    hints = ColumnHints(date_col="fecha", amount_col="monto", desc_col="desc", id_col="__id__")
    meta = {"source": origen, "rows": len(prepared), "hints": hints.__dict__}
    return prepared, meta


def _candidate_score(desc_a: str, desc_b: str, date_diff_days: float) -> float:
    sim = fuzz.token_set_ratio(desc_a or "", desc_b or "") / 100.0
    # Penalizar diferencia de fecha
    penalty = min(abs(date_diff_days) / 30.0, 1.0)
    return sim * (1.0 - 0.3 * penalty)


def _find_matches_for_row(row: pd.Series, libros: pd.DataFrame, hints: ColumnHints, window_days: int = 2, tol: float = 1.00) -> List[Tuple[int, float]]:
    date_col = hints.date_col
    amount_col = hints.amount_col
    desc_col = hints.desc_col

    amount = row.get(amount_col)
    date = row.get(date_col)
    desc = row.get(desc_col, "")

    if amount is None or pd.isna(amount):
        return []

    lo = amount - tol
    hi = amount + tol

    candidates = libros[(libros[amount_col] >= lo) & (libros[amount_col] <= hi)].copy()
    if date is not None and not pd.isna(date):
        if date_col in libros.columns:
            candidates = candidates[(candidates[date_col] >= (date - pd.Timedelta(days=window_days))) & (candidates[date_col] <= (date + pd.Timedelta(days=window_days)))]

    if candidates.empty:
        return []

    scores: List[Tuple[int, float]] = []
    for idx, r in candidates.iterrows():
        d2 = r.get(date_col)
        ddays = (d2 - date).days if (date is not None and d2 is not None and not pd.isna(date) and not pd.isna(d2)) else 9999
        score = _candidate_score(str(desc), str(r.get(desc_col, "")), ddays)
        scores.append((idx, score))

    scores.sort(key=lambda x: x[1], reverse=True)
    return scores


def multipass_match(extracto: pd.DataFrame, ventas: pd.DataFrame, compras: pd.DataFrame) -> Dict[int, Dict[str, Any]]:
    # Asumimos que extracto/ventas/compras ya vienen coerced con hints compatibles
    # Para simplicity, usamos hints del extracto para nombres de columnas
    from .normalize import detect_columns

    hints_e = detect_columns(extracto)
    hints_v = detect_columns(ventas)
    hints_c = detect_columns(compras)

    results: Dict[int, Dict[str, Any]] = {}

    # Buckets por importe redondeado (sin centavos) para acelerar
    def bucket(df: pd.DataFrame, amount_col: str) -> pd.Series:
        return df[amount_col].fillna(0.0).round(0)

    ventas_b = bucket(ventas, hints_v.amount_col)
    compras_b = bucket(compras, hints_c.amount_col)

    # Intentar primero contra Ventas, luego Compras
    for _, row in extracto.iterrows():
        rid = int(row["__id__"])
        # Signo: credito -> Ventas, debito -> Compras
        tipo = str(row.get("tipo", ""))
        target_first = "Ventas" if tipo.lower().startswith("cred") else "Compras" if tipo.lower().startswith("deb") else None

        def pick(df: pd.DataFrame, hints: ColumnHints, buck: pd.Series) -> List[Tuple[int, float]]:
            # Preselección por bucket de monto
            bval = round(float(row.get(hints_e.amount_col) or 0.0))
            subset_idx = buck[buck == bval].index
            if len(subset_idx) == 0:
                return []
            subset = df.loc[subset_idx]
            return _find_matches_for_row(row, subset, hints)

        matches_v = pick(ventas, hints_v, ventas_b)
        matches_c = pick(compras, hints_c, compras_b)

        chosen = None
        chosen_src = None

        if (target_first == "Ventas" and matches_v) or (target_first is None and matches_v):
            # Top-N y reranking IA
            top_ids = [i for i, _ in matches_v[:5]]
            cands = [
                {
                    "descripcion": str(ventas.loc[i, hints_v.desc_col]) if hints_v.desc_col in ventas.columns else "",
                    "monto": ventas.loc[i, hints_v.amount_col] if hints_v.amount_col in ventas.columns else None,
                    "fecha": str(ventas.loc[i, hints_v.date_col]) if hints_v.date_col in ventas.columns else None,
                }
                for i in top_ids
            ]
            order = rerank_candidates_with_ai(str(row.get(hints_e.desc_col, "")), cands)
            chosen = top_ids[order[0]] if order else top_ids[0]
            chosen_src = "Ventas"
        elif (target_first == "Compras" and matches_c) or (target_first is None and matches_c):
            top_ids = [i for i, _ in matches_c[:5]]
            cands = [
                {
                    "descripcion": str(compras.loc[i, hints_c.desc_col]) if hints_c.desc_col in compras.columns else "",
                    "monto": compras.loc[i, hints_c.amount_col] if hints_c.amount_col in compras.columns else None,
                    "fecha": str(compras.loc[i, hints_c.date_col]) if hints_c.date_col in compras.columns else None,
                }
                for i in top_ids
            ]
            order = rerank_candidates_with_ai(str(row.get(hints_e.desc_col, "")), cands)
            chosen = top_ids[order[0]] if order else top_ids[0]
            chosen_src = "Compras"

        if chosen is not None:
            results[rid] = {"match_index": int(chosen), "source": chosen_src}

    return results


def build_output_sheet(original_extracto: pd.DataFrame, prepared_extracto: pd.DataFrame, matches: Dict[int, Dict[str, Any]]) -> pd.DataFrame:
    result = prepared_extracto.copy()
    # Columnas de salida solicitadas
    for col in ["Conciliado", "Origen", "NroComprobante", "FechaLibro", "ImporteLibro", "Diferencia", "ReglaAplicada"]:
        if col not in result.columns:
            result[col] = ""

    for i, row in result.iterrows():
        rid = int(row["__id__"])
        m = matches.get(rid)
        if m:
            source = m.get("source", "")
            result.at[i, "Conciliado"] = "Si"
            result.at[i, "Origen"] = source
            # Completar datos del libro
            # Nota: en esta versión base no preservamos el dataset de ventas/compras aquí
            # para completar comprobante/fecha/importe; esa mejora se puede añadir guardando
            # referencias en matches. Dejamos placeholders.
            result.at[i, "ReglaAplicada"] = "multipass"
        else:
            result.at[i, "Conciliado"] = "No"

    return result


