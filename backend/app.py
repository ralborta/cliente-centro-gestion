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
    async def read_any(f: UploadFile) -> pd.DataFrame:
        content = await f.read()
        bio = io.BytesIO(content)
        name = (f.filename or "").lower()
        if name.endswith(".csv"):
            return pd.read_csv(bio, dtype=str)
        return pd.read_excel(bio, dtype=str)

    ext_df = await read_any(extracto)
    ven_df = await read_any(ventas)
    com_df = await read_any(compras)

    # Minimal pipeline – core functions can be expanded later
    E, _ = prep_extracto(ext_df)
    V, _ = prep_libro(ven_df, origen="Ventas")
    C, _ = prep_libro(com_df, origen="Compras")
    best = multipass_match(E, V, C)
    sheet = build_output_sheet(ext_df, E, best)

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


