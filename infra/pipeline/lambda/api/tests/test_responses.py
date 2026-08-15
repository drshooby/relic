import json
import pytest
from decimal import Decimal

from responses import json_default, no_content, not_found, ok

CORS_ORIGIN = "http://localhost:5173"


def test_ok_returns_200_with_json_body_and_cors():
    resp = ok({"sessions": []})
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"]) == {"sessions": []}
    assert resp["headers"]["Access-Control-Allow-Origin"] == CORS_ORIGIN
    assert resp["headers"]["Content-Type"] == "application/json"


def test_no_content_returns_204_with_no_body():
    resp = no_content()
    assert resp["statusCode"] == 204
    # 204 forbids a body. Returning one is a protocol violation, and API
    # Gateway may pass it through to the browser, where fetch's res.json()
    # would then behave inconsistently across runtimes.
    assert "body" not in resp
    assert resp["headers"]["Access-Control-Allow-Origin"] == CORS_ORIGIN


def test_not_found_returns_404_with_a_message():
    resp = not_found("no such session")
    assert resp["statusCode"] == 404
    assert json.loads(resp["body"]) == {"error": "no such session"}


def test_decimal_serializes_without_raising():
    # DynamoDB returns every Number as decimal.Decimal, and json.dumps raises
    # TypeError on it. This is the mirror image of the hot path's float bug:
    # the same type boundary, failing in the opposite direction.
    resp = ok({"game_time_s": Decimal("186.318")})
    assert json.loads(resp["body"])["game_time_s"] == 186.318


def test_integral_decimal_serializes_as_int_not_float():
    # event_count comes back as Decimal("6372"). Rendering it as 6372.0 in the
    # UI would look like a bug.
    resp = ok({"event_count": Decimal("6372")})
    assert json.loads(resp["body"])["event_count"] == 6372
    assert "6372.0" not in resp["body"]


def test_json_default_raises_on_genuinely_unserializable_types():
    # A silent fallback to str() would hide real bugs.
    with pytest.raises(TypeError):
        json_default(object())
