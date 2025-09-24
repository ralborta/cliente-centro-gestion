cliente-centro-gestion

Monorepo: frontend (Next.js en Vercel) + backend (FastAPI en Railway/Render/Cloud Run).

Local

- Backend:
  - Python 3.11
  - cd backend && python -m venv .venv && source .venv/bin/activate
  - pip install -r requirements.txt
  - uvicorn app:app --reload --port 8080

- Frontend:
  - cd frontend
  - Crear .env.local con: NEXT_PUBLIC_API_URL=http://localhost:8080
  - npm install && npm run dev

Deploy

- Backend: con Dockerfile incluido. En Railway: crear servicio desde repo. Health: /health, puerto 8080.
- Frontend en Vercel: apuntar a frontend/ y definir NEXT_PUBLIC_API_URL al dominio del backend.
- CORS: en backend/app.py, añadir tu dominio de Vercel en ALLOWED_ORIGINS.


