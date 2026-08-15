"""API Gateway v2 proxy responses. Pure: no AWS, no I/O.

Every response carries CORS headers. The browser calls this API from
http://localhost:5173, a different origin from execute-api, so without them
the fetch fails before the handler's status code is ever read.
"""

import json
from decimal import Decimal

# Phase 1 runs the dashboard locally. Not a wildcard: CORS is what stops other
# sites' JS from reading your session telemetry while the pipeline is applied.
CORS_ORIGIN = "http://localhost:5173"

_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": CORS_ORIGIN,
}


def json_default(value):
    """Convert types json.dumps cannot handle.

    DynamoDB returns every Number as decimal.Decimal. Integral values become
    int so event_count renders as 6372 rather than 6372.0; the rest become
    float. Anything else raises, because a silent str() fallback would hide
    real bugs.
    """
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def ok(body: dict) -> dict:
    return {
        "statusCode": 200,
        "headers": _HEADERS,
        "body": json.dumps(body, default=json_default),
    }


def no_content() -> dict:
    # No "body" key at all: 204 forbids one.
    return {"statusCode": 204, "headers": _HEADERS}


def not_found(message: str) -> dict:
    return {
        "statusCode": 404,
        "headers": _HEADERS,
        "body": json.dumps({"error": message}),
    }
