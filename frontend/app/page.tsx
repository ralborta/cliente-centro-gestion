"use client";
import { useState } from "react";
import { postReconcile } from "@/lib/api";

export default function Home() {
  const [extracto, setExtracto] = useState<File | null>(null);
  const [ventas, setVentas] = useState<File | null>(null);
  const [compras, setCompras] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const apiUrl = process.env.NEXT_PUBLIC_API_URL!;

  const onRun = async () => {
    if (!extracto || !ventas || !compras) return alert("Subí los 3 archivos");
    if (!apiUrl) return alert("Definí NEXT_PUBLIC_API_URL");
    setLoading(true);
    try {
      const blob = await postReconcile(apiUrl, { extracto, ventas, compras });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "conciliado.xlsx";
      a.click();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      alert(e?.message || "Error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <main style={{ maxWidth: 720, margin: "40px auto", fontFamily: "system-ui" }}>
      <h1>Conciliador</h1>
      <p>
        Subí un extracto + libros de ventas y compras. Te devuelvo el Excel conciliado.
      </p>
      <div style={{ display: "grid", gap: 12, marginTop: 16 }}>
        <input type="file" onChange={(e) => setExtracto(e.target.files?.[0] || null)} />
        <input type="file" onChange={(e) => setVentas(e.target.files?.[0] || null)} />
        <input type="file" onChange={(e) => setCompras(e.target.files?.[0] || null)} />
        <button onClick={onRun} disabled={loading}>
          {loading ? "Procesando..." : "Conciliar"}
        </button>
        {!apiUrl && <p style={{ color: "tomato" }}>Definí NEXT_PUBLIC_API_URL</p>}
      </div>
    </main>
  );
}


