# HTTP API, not REST API: cheaper per request, and none of REST's extras
# (usage plans, API keys, request validators) are needed here.
resource "aws_apigatewayv2_api" "relic_api" {
  name          = "relic-api"
  protocol_type = "HTTP"

  # The browser calls this from http://localhost:5173, a different origin, so
  # without CORS the fetch fails before any status code is read. 5173 is Vite's
  # dev-server port -- a mismatch here fails every browser request with an
  # opaque CORS error while curl keeps working.
  #
  # Not a wildcard: this is what stops another site's JS from reading session
  # telemetry while the pipeline is applied. CORS is browser-only and is NOT
  # what makes the API private -- there is no auth, by design (phase-1 design
  # section 6). The real mitigations are that this stack exists only while you
  # are playing and serves a 24h rolling cache.
  cors_configuration {
    allow_origins = ["http://localhost:5173"]
    allow_methods = ["GET"]
    allow_headers = ["content-type"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.relic_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "list_sessions" {
  api_id    = aws_apigatewayv2_api.relic_api.id
  route_key = "GET /sessions"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

# {id} does not match across a "/", and there is deliberately no $default
# route, so anything matching neither template is rejected by API Gateway
# before it ever reaches the Lambda.
resource "aws_apigatewayv2_route" "session_events" {
  api_id    = aws_apigatewayv2_api.relic_api.id
  route_key = "GET /sessions/{id}/events"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

# auto_deploy avoids a separate deployment resource that must be tainted on
# every change.
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.relic_api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"

  # apigateway.amazonaws.com is global. Without source_arn, any account's API
  # Gateway could invoke this function -- the confused-deputy problem.
  source_arn = "${aws_apigatewayv2_api.relic_api.execution_arn}/*/*"
}
