"""
Cloud Run entrypoint for the Mock PI Web API server.

This module wraps the core PI Web API mock (`mock_piwebapi.pi_web_api.app`) and enforces
optional Bearer-token authentication for all endpoints.
"""

import os

from fastapi import Request
from fastapi.responses import JSONResponse

from mock_piwebapi.pi_web_api import app as _app


EXPECTED_BEARER_TOKEN = os.getenv("EXPECTED_BEARER_TOKEN", "").strip()

app = _app


@app.middleware("http")
async def require_bearer_auth(request: Request, call_next):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"error": "missing_bearer_token"})

    token = auth.split(" ", 1)[1].strip()
    if not token:
        return JSONResponse(status_code=401, content={"error": "empty_bearer_token"})

    # If EXPECTED_BEARER_TOKEN isn't configured, accept any non-empty bearer token.
    if EXPECTED_BEARER_TOKEN and token != EXPECTED_BEARER_TOKEN:
        return JSONResponse(status_code=403, content={"error": "invalid_bearer_token"})

    return await call_next(request)

