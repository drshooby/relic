data "archive_file" "firehose_func_files" {
  type        = "zip"
  source_file = "${path.module}/lambda/firehose/main.py"
  output_path = "${path.module}/lambda/firehose/main.zip"
}

resource "aws_lambda_function" "lambda_processor" {
  filename         = data.archive_file.firehose_func_files.output_path
  source_code_hash = data.archive_file.firehose_func_files.output_base64sha256
  function_name    = "firehose_lambda_processor"
  role             = aws_iam_role.firehose_lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.12"
}
