from __future__ import annotations

from typing import List
import io
import pdfplumber
import pandas as pd


def pdf_to_rows(pdf_bytes: bytes) -> List[List[str]]:
    """
    Convierte un PDF en una lista de filas (muy básico).
    Para tabulados complejos considerar camelot/tabula.
    """
    rows: List[List[str]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table() or []
            for row in table:
                rows.append([str(cell) if cell is not None else "" for cell in row])
    return rows


def pdf_to_dataframe(pdf_bytes: bytes) -> pd.DataFrame:
    rows = pdf_to_rows(pdf_bytes)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


