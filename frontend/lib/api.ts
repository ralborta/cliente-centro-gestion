export async function postReconcile(
  apiUrl: string | null,
  files: { extracto: File; ventas: File; compras: File }
) {
  const fd = new FormData();
  fd.append("extracto", files.extracto);
  fd.append("ventas", files.ventas);
  fd.append("compras", files.compras);

  // Si hay API_URL usaremos backend directo; si no, usamos el proxy local
  const target = apiUrl ? `${apiUrl}/reconcile` : "/api/reconcile";

  const res = await fetch(target, {
    method: "POST",
    body: fd,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Error conciliando");
  }
  return await res.blob();
}


