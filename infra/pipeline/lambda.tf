data "archive_file" "hot_path_func_files" {
  type = "zip"
  # source_dir, not source_file: the handler imports parser.py and items.py,
  # and a single-file zip would deploy fine and then ImportError at cold start.
  source_dir  = "${path.module}/lambda/hot-path"
  output_path = "${path.module}/lambda/hot-path.zip"

  # Tests, tooling, and caches must not ship to Lambda. output_path sits
  # outside source_dir so the archive never tries to include itself.
  excludes = [
    "tests",
    "pyproject.toml",
    "uv.lock",
    "__pycache__",
    ".pytest_cache",
    ".venv",
  ]
}

resource "aws_lambda_function" "hot_path" {
  filename         = data.archive_file.hot_path_func_files.output_path
  source_code_hash = data.archive_file.hot_path_func_files.output_base64sha256
  function_name    = "relic-hot-path"
  role             = aws_iam_role.hot_path_lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.12"

  # Table names reach the handler as env vars rather than being hardcoded, so
  # the Python has no knowledge of Terraform naming.
  environment {
    variables = {
      EVENTS_TABLE   = aws_dynamodb_table.events.name
      SESSIONS_TABLE = aws_dynamodb_table.sessions.name
    }
  }
}

data "archive_file" "api_func_files" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/api"
  output_path = "${path.module}/lambda/api.zip"

  excludes = [
    "tests",
    "pyproject.toml",
    "uv.lock",
    "__pycache__",
    ".pytest_cache",
    ".venv",
  ]
}

resource "aws_lambda_function" "api" {
  filename         = data.archive_file.api_func_files.output_path
  source_code_hash = data.archive_file.api_func_files.output_base64sha256
  function_name    = "relic-api"
  role             = aws_iam_role.api_lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.12"

  environment {
    variables = {
      EVENTS_TABLE   = aws_dynamodb_table.events.name
      SESSIONS_TABLE = aws_dynamodb_table.sessions.name
    }
  }
}
