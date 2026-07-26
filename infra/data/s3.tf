resource "aws_s3_bucket" "data_bucket" {
  bucket = "relic-data-bucket-${random_id.bucket_suffix.hex}"
}

# Raw EE.log lines are PII-adjacent and land only here. Never widen this.
resource "aws_s3_bucket_public_access_block" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The cold path is the replayable source of truth, so an accidental
# same-key write should not destroy history.
resource "aws_s3_bucket_versioning" "data_bucket" {
  bucket = aws_s3_bucket.data_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Resource-side guardrail: closed by default.
#
# No principal is granted anything here. The bucket's writer is Firehose, which
# lives in the ephemeral infra/pipeline stack and carries its own delivery role
# (EE.log -> operator -> Kinesis -> Firehose -> here). Grants belong on that
# role; this policy only denies things no principal should ever be allowed.
#
# Both statements are Deny-only on purpose. A bucket policy that denies
# Principal "*" unconditionally would also lock this account out of
# s3:PutBucketPolicy, which is unrecoverable without root -- so the blanket
# deny is scoped by condition, never by a bare wildcard.
data "aws_iam_policy_document" "data_bucket_policy" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.data_bucket.arn,
      "${aws_s3_bucket.data_bucket.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # Deny-all-but-this-account. Until the pipeline stack exists there is no
  # in-account writer, so in practice this denies everyone; once Firehose is
  # created it passes this statement and is gated by its delivery role instead.
  statement {
    sid     = "DenyOutsideThisAccount"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.data_bucket.arn,
      "${aws_s3_bucket.data_bucket.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotEquals"
      variable = "aws:PrincipalAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_s3_bucket_policy" "data_bucket_policy" {
  bucket = aws_s3_bucket.data_bucket.id
  policy = data.aws_iam_policy_document.data_bucket_policy.json
}