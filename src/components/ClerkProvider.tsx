/**
 * Clerk Authentication Provider for Aradune
 *
 * Wraps the app with Clerk auth when VITE_CLERK_PUBLISHABLE_KEY is set.
 * Falls back to the legacy password gate ("mediquiad") when Clerk is not configured.
 *
 * Usage in Platform.tsx:
 *   <AuthGate>
 *     <App />
 *   </AuthGate>
 */

import { ClerkProvider as ClerkProviderBase, SignIn, SignedIn, SignedOut, UserButton, useUser, useAuth } from "@clerk/clerk-react";
import { useEffect, type ReactNode } from "react";
import { setClerkTokenGetter } from "../lib/api";

// ── Clerk configuration ─────────────────────────────────────────────────
const CLERK_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY as string | undefined;

/** True when Clerk is configured (publishable key is set and non-empty) */
export const isClerkEnabled = Boolean(CLERK_KEY && CLERK_KEY.length > 0);

// ── ClerkTokenSync — registers the Clerk token getter with the API module ──
function ClerkTokenSync({ children }: { children: ReactNode }) {
  const { getToken } = useAuth();

  useEffect(() => {
    // Register the Clerk getToken function so api.ts can include
    // the session JWT in Authorization headers for protected endpoints
    setClerkTokenGetter(() => getToken());
    return () => setClerkTokenGetter(() => Promise.resolve(null));
  }, [getToken]);

  return <>{children}</>;
}

// ── ClerkAuthProvider — wraps children with Clerk context ───────────────
export function ClerkAuthProvider({ children }: { children: ReactNode }) {
  if (!isClerkEnabled) {
    // No Clerk key — just render children directly (password gate handles auth)
    return <>{children}</>;
  }

  return (
    <ClerkProviderBase
      publishableKey={CLERK_KEY!}
      appearance={{
        variables: {
          colorPrimary: "#2E6B4A",
          colorText: "#0A2540",
          fontFamily: "'Helvetica Neue',Arial,sans-serif",
          borderRadius: "6px",
        },
        elements: {
          card: { boxShadow: "0 4px 16px rgba(0,0,0,.06),0 12px 40px rgba(0,0,0,.04)" },
          formButtonPrimary: { backgroundColor: "#2E6B4A", fontWeight: 600 },
        },
      }}
    >
      <ClerkTokenSync>{children}</ClerkTokenSync>
    </ClerkProviderBase>
  );
}

// ── RequireAuth — shows sign-in if not authenticated ────────────────────
export function RequireAuth({ children, fallback }: { children: ReactNode; fallback?: ReactNode }) {
  if (!isClerkEnabled) {
    // Clerk not configured — caller handles auth (password gate)
    return <>{children}</>;
  }

  return (
    <>
      <SignedIn>{children}</SignedIn>
      <SignedOut>
        {fallback ?? (
          <div style={{
            minHeight: "100vh",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            fontFamily: "'Helvetica Neue',Arial,sans-serif",
            background: "#FAFBFA",
            padding: "0 20px",
          }}>
            <div style={{ marginBottom: 32 }}>
              <div style={{
                fontSize: 32, fontWeight: 700, letterSpacing: 3,
                color: "#2E6B4A", textTransform: "uppercase",
              }}>
                ARADUNE
              </div>
            </div>
            <SignIn
              routing="hash"
              fallbackRedirectUrl="/"
              appearance={{
                elements: {
                  rootBox: { width: "100%", maxWidth: 400 },
                  card: { boxShadow: "0 4px 16px rgba(0,0,0,.06),0 12px 40px rgba(0,0,0,.04)", border: "1px solid #E4EAE4" },
                },
              }}
            />
            <div style={{ marginTop: 48, fontSize: 11, color: "#425A70" }}>
              Questions?{" "}
              <a href="mailto:aradune-medicaid@proton.me" style={{ color: "#2E6B4A", textDecoration: "none" }}>
                aradune-medicaid@proton.me
              </a>
            </div>
          </div>
        )}
      </SignedOut>
    </>
  );
}

// ── UserNav — avatar + name for the nav bar ─────────────────────────────
export function UserNav() {
  if (!isClerkEnabled) return null;

  return (
    <SignedIn>
      <UserNavInner />
    </SignedIn>
  );
}

function UserNavInner() {
  const { user } = useUser();

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginLeft: 4 }}>
      {user && (
        <span style={{
          fontSize: 11,
          color: "#425A70",
          fontFamily: "'Helvetica Neue',Arial,sans-serif",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          maxWidth: 100,
        }}>
          {user.firstName || user.primaryEmailAddress?.emailAddress?.split("@")[0] || ""}
        </span>
      )}
      <UserButton
        appearance={{
          elements: {
            avatarBox: { width: 28, height: 28 },
            userButtonTrigger: { borderRadius: "50%" },
          },
        }}
      />
    </div>
  );
}
