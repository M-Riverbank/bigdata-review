import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Sidebar from "@/components/layout/Sidebar";
import Topbar from "@/components/layout/Topbar";
import { getAllArticles } from "@/lib/content";
import ErrorBoundary from "@/components/ErrorBoundary";
import type { Article } from "@/types";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "大数据面试备战冲刺网",
  description: "大数据开发面试复习平台 — 组件教程 + 互动题库 + AI 判题",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  let allArticles: Article[] = [];
  try {
    allArticles = getAllArticles();
  } catch (e) {
    console.error('Failed to load articles:', e);
  }

  return (
    <html
      lang="zh-CN"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased dark`}
    >
      <body className="min-h-full bg-gray-950 text-gray-100">
        <Sidebar articles={allArticles} />
        <div className="ml-60 flex flex-col min-h-screen">
          <Topbar />
          <main className="flex-1 p-6">
            <ErrorBoundary>
              {children}
            </ErrorBoundary>
          </main>
        </div>
      </body>
    </html>
  );
}
