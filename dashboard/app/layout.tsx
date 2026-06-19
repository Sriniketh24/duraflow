import type { Metadata } from 'next';
import './globals.css';
import NavBar from './nav-bar';

export const metadata: Metadata = {
  title: 'Duraflow Dashboard',
  description: 'Operations dashboard for the Duraflow durable workflow engine',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <div className="app-shell">
          <NavBar />
          <main className="content">{children}</main>
        </div>
      </body>
    </html>
  );
}
