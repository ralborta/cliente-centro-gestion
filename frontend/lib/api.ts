export async function postReconcile(
  apiUrl: string,
  files: { extracto: File; ventas: File; compras: File }
) {
  const fd = new FormData();
  fd.append("extracto", files.extracto);
  fd.append("ventas", files.ventas);
  fd.append("compras", files.compras);

  const res = await fetch(`${apiUrl}/reconcile`, {
    method: "POST",
    body: fd,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Error conciliando");
  }
  return await res.blob();
}


