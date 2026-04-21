export const metadata = {
  title: 'RIA Signal Dashboard',
  description: 'RIA signal dashboard MVP'
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
