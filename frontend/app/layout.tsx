export const metadata = {
  title: "Conciliador",
  description: "Subí extracto + libros y descargá Excel conciliado",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="es">
      <body>{children}</body>
    </html>
  );
}


