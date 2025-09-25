from __future__ import annotations

import os
from typing import List, Dict, Any

try:
    from openai import OpenAI
except Exception:  # openai opcional
    OpenAI = None  # type: ignore


def rerank_candidates_with_ai(query: str, candidates: List[Dict[str, Any]]) -> List[int]:
    """
    Devuelve los índices de candidates ordenados por relevancia segun IA.
    Si no hay OPENAI_API_KEY, retorna el orden original.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return list(range(len(candidates)))

    try:
        client = OpenAI(api_key=api_key)
        # Prompt simple: pedimos que ordene por similitud con 'query'
        items = [f"[{i}] {c.get('descripcion', '')} (monto={c.get('monto')}, fecha={c.get('fecha')})" for i, c in enumerate(candidates)]
        prompt = (
            "Ordena por mejor coincidencia con la transacción de extracto '" + query + "'\n" +
            "Opciones:\n" + "\n".join(items) +
            "\nDevuelve una lista de índices en orden, separados por comas."
        )
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = res.choices[0].message.content or ""
        numbers = []
        for token in text.replace("[", " ").replace("]", " ").split(","):
            token = token.strip()
            if token.isdigit():
                numbers.append(int(token))
        if numbers:
            # Filtrar índices válidos
            return [i for i in numbers if 0 <= i < len(candidates)]
        return list(range(len(candidates)))
    except Exception:
        return list(range(len(candidates)))


