data "archive_file" "hot_path_func_files" {
  type        = "zip"
  source_file = "${path.module}/lambda/hot-path/main.py"
  output_path = "${path.module}/lambda/hot-path/main.zip"
}

resource "aws_lambda_function" "hot_path" {
  filename         = data.archive_file.hot_path_func_files.output_path
  source_code_hash = data.archive_file.hot_path_func_files.output_base64sha256
  function_name    = "relic-hot-path"
  role             = aws_iam_role.hot_path_lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.12"
}
