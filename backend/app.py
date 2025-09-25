from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import io
import pandas as pd

# Core reconciliation utilities (minimal placeholders to keep service functional)
from core.matcher import (
    prep_extracto,
    prep_libro,
    multipass_match,
    build_output_sheet,
    ai_only_match,
)

app = FastAPI(title="Conciliador")

# Configure CORS for Vercel domain and local dev
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://cliente-centro-gestion-myu99unkh-nivel-41.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=r"https://.*\\.vercel\\.app$",
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    expose_headers=["Content-Disposition"],
    max_age=86400,
)


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.post("/reconcile")
async def reconcile(
    extracto: UploadFile = File(...),
    ventas: UploadFile = File(...),
    compras: UploadFile = File(...),
):
    async def read_any(f: UploadFile, kind: str) -> pd.DataFrame:
        content = await f.read()
        bio = io.BytesIO(content)
        name = (f.filename or "").lower()
        if name.endswith(".csv"):
            return pd.read_csv(bio, dtype=str)
        # Excel: intentar seleccionar hoja por nombre segun tipo
        try:
            xls = pd.ExcelFile(bio)
            sheets = [s.lower() for s in xls.sheet_names]
            preferred = []
            if kind == "extracto":
                preferred = ["movimientos", "extracto", "sheet1", "hoja1"]
            elif kind == "ventas":
                preferred = ["hoja1", "ventas", "sheet1"]
            elif kind == "compras":
                preferred = ["hoja1", "compras", "sheet1"]
            pick = next((orig for s, orig in zip(sheets, xls.sheet_names) if s in preferred), None)
            if pick is None:
                pick = xls.sheet_names[0]
            return pd.read_excel(xls, sheet_name=pick, dtype=str)
        except Exception:
            # fallback lectura directa
            bio.seek(0)
            return pd.read_excel(bio, dtype=str)

    ext_df = await read_any(extracto, "extracto")
    ven_df = await read_any(ventas, "ventas")
    com_df = await read_any(compras, "compras")

    # Minimal pipeline – core functions can be expanded later
    E, _ = prep_extracto(ext_df)
    V, _ = prep_libro(ven_df, origen="Ventas")
    C, _ = prep_libro(com_df, origen="Compras")
    # Si hay OPENAI_API_KEY y se solicita IA, usar AI-only
    import os
    use_ai_only = bool(os.getenv("OPENAI_API_KEY"))
    if use_ai_only:
        best = ai_only_match(E, V, C)
    else:
        best = multipass_match(E, V, C)
    sheet = build_output_sheet(ext_df, E, best, ventas=V, compras=C)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        sheet.to_excel(writer, index=False, sheet_name="Extracto")
    out.seek(0)

    return StreamingResponse(
        out,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": 'attachment; filename="conciliado.xlsx"'
        },
    )


