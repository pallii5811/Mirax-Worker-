import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Lead Gen & Audit Machine",
  description: "Premium Lead Generation & Marketing Audit",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
