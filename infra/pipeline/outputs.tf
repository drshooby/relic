output "api_invoke_url" {
  description = "Base URL for the read API. Put this in dashboard/.env.local as VITE_RELIC_API_URL. It changes on every apply, because the pipeline is destroyed between play sessions."
  value       = aws_apigatewayv2_stage.default.invoke_url
}
