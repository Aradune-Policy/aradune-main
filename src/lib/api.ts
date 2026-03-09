/**
 * Shared API client for FastAPI backend.
 * Uses VITE_API_URL when set (e.g. Fly.io), otherwise falls back to
 * same-origin which works with static JSON in public/data/.
 */

const API_BASE = import.meta.env.VITE_API_URL || "https://aradune-api.fly.dev";

export async function apiFetch<T>(path: string, fallbackPath?: string): Promise<T> {
  // Try the API endpoint first
  try {
    const res = await fetch(`${API_BASE}${path}`);
    if (res.ok) return res.json();
  } catch {
    // API unreachable — fall through to fallback
  }

  // If a static JSON fallback is provided, try it
  if (fallbackPath) {
    const res = await fetch(fallbackPath);
    if (res.ok) return res.json();
  }

  throw new Error(`API request failed: ${path}`);
}

export { API_BASE };
