"""
Adversarial testing configuration.
Point at local dev or production.
"""

import os

# Target environment (supports both env var names for compatibility)
API_BASE = os.environ.get("ARADUNE_API_BASE",
           os.getenv("ARADUNE_TEST_URL", "http://localhost:8000"))
FRONTEND_BASE = os.environ.get("ARADUNE_FRONTEND_BASE",
                os.getenv("ARADUNE_FRONTEND_URL", "http://localhost:5173"))

# Auth (if Clerk is active, supply a test JWT; otherwise dev mode skips auth)
AUTH_HEADER = os.getenv("ARADUNE_TEST_AUTH", "")

# Claude API for agent reasoning
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Thresholds
INTELLIGENCE_TIMEOUT_S = 60
API_TIMEOUT_S = 15
MAX_ACCEPTABLE_500_RATE = 0.0
CONSISTENCY_TOLERANCE_PCT = 10.0
MIN_INTELLIGENCE_PASS_RATE = 0.85
LATENCY_WARNING_S = 30.0          # flag responses slower than this

# Limits
MAX_QUERIES_PER_AGENT = 50
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
