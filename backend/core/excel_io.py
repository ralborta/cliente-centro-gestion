from __future__ import annotations

import io
import pandas as pd


def write_excel_multiple(sheets: dict[str, pd.DataFrame]) -> io.BytesIO:
    """
    Escribe múltiples hojas a un Excel en memoria. No se usa en la ruta mínima
    pero queda disponible para futuros endpoints.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=str(name)[:31])
    buffer.seek(0)
    return buffer


