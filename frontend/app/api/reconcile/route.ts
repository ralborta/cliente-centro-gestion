export const runtime = "nodejs";

export async function POST(request: Request) {
  try {
    const form = await request.formData();
    const backendUrl = process.env.BACKEND_URL || process.env.NEXT_PUBLIC_API_URL;
    if (!backendUrl) {
      return new Response("BACKEND_URL no configurado", { status: 500 });
    }

    const res = await fetch(`${backendUrl}/reconcile`, {
      method: "POST",
      body: form,
    });

    if (!res.ok) {
      const text = await res.text();
      return new Response(text || "Error en backend", { status: res.status });
    }

    // Reemitimos el stream y headers para descarga del Excel
    const headers = new Headers(res.headers);
    const contentType = headers.get("content-type") ||
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    const dispo = headers.get("content-disposition") ||
      'attachment; filename="conciliado.xlsx"';

    return new Response(res.body, {
      status: 200,
      headers: {
        "Content-Type": contentType,
        "Content-Disposition": dispo,
      },
    });
  } catch (err: any) {
    return new Response(err?.message || "Error proxy", { status: 500 });
  }
}


