/**
 * Shared API client for FastAPI backend.
 * Uses VITE_API_URL when set (e.g. Fly.io), otherwise falls back to
 * same-origin which works with static JSON in public/data/.
 */

const API_BASE = (import.meta.env.VITE_API_URL || "https://aradune-api.fly.dev").trim();

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

/**
 * Get the Clerk session token for authenticated API requests.
 * Returns null if Clerk is not configured or user is not signed in.
 *
 * This dynamically imports Clerk to avoid circular dependencies
 * and only runs when Clerk is actually configured.
 */
let _getTokenFn: (() => Promise<string | null>) | null = null;

export function setClerkTokenGetter(fn: () => Promise<string | null>) {
  _getTokenFn = fn;
}

export async function getAuthToken(): Promise<string | null> {
  if (!_getTokenFn) return null;
  try {
    return await _getTokenFn();
  } catch {
    return null;
  }
}

/**
 * Build auth headers for API requests.
 * Returns an object with the Authorization header if a Clerk token is available.
 */
export async function getAuthHeaders(): Promise<Record<string, string>> {
  const token = await getAuthToken();
  if (token) {
    return { Authorization: `Bearer ${token}` };
  }
  return {};
}

export { API_BASE };
