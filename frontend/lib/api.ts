export async function postReconcile(
  _apiUrl: string | null,
  files: { extracto: File; ventas: File; compras: File }
) {
  const fd = new FormData();
  fd.append("extracto", files.extracto);
  fd.append("ventas", files.ventas);
  fd.append("compras", files.compras);

  // Usamos siempre el proxy local para evitar CORS/405
  const target = "/api/reconcile";

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


