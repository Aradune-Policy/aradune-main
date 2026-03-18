# ADR 005: Authentication Architecture (Clerk + Password Gate Fallback)

## Status
Accepted (2025-03)

## Context
Aradune needs authentication for the Intelligence endpoint (to control AI costs) while keeping data browsing endpoints public (to serve the free tier). The options were: (1) build custom auth, (2) use an auth-as-a-service provider, (3) use API keys.

Custom auth is a security liability for a solo developer. API keys don't support user management, SSO, or audit trails required for enterprise sales.

## Decision
Use Clerk as the authentication provider. Clerk handles user management, session tokens (JWT), SSO, and provides React components for sign-in/sign-up.

Implementation:
- Frontend: ClerkProvider wraps the app. Clerk session tokens are sent as Bearer tokens.
- Backend: FastAPI middleware validates Clerk JWTs via JWKS endpoint. When CLERK_SECRET_KEY is not set, auth falls back to a client-side password gate ("mediquiad") for development.
- Protected endpoints: Intelligence (/api/intelligence), file import (/api/import), skillbook admin.
- Public endpoints: All data browsing (/api/states, /api/rates, etc.).

## Consequences
- Positive: Enterprise-grade auth with zero security code to maintain. SOC 2 compliant. Supports Google OAuth, email, and future SSO.
- Positive: Graceful fallback to password gate when Clerk is not configured. Development works without Clerk keys.
- Negative: Vendor dependency. $25/month at scale (>1000 MAU). Acceptable for the value provided.
- Negative: JWT verification adds ~5ms per protected request. Negligible compared to DuckDB query time.
- Open issue: Frontend publishable key and backend secret key must be from the same Clerk application instance. Mismatched keys cause "Authentication error" (Session 34 known issue).
