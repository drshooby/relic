# No principals are defined in this stack.
#
# The only thing that writes to the data bucket is Firehose, and its delivery
# role belongs in infra/pipeline: it is torn down with the rest of the pipeline
# on every scripts/down.sh, so it must not live in the persistent stack.
#
# When that role is built it needs, scoped to this bucket:
#   s3:PutObject                      write the raw archive objects
#   s3:AbortMultipartUpload           Firehose buffers large batches
#   s3:GetBucketLocation, s3:ListBucket   on the bucket ARN, not bucket/*
#
# Note that Firehose is stricter than a plain writer -- PutObject alone is not
# enough. It also needs a trust policy for firehose.amazonaws.com, ideally with
# an aws:SourceAccount condition to avoid the confused-deputy problem.
#
# It does NOT need s3:DeleteObject. The cold path is the replayable source of
# truth; nothing in the pipeline should be able to remove history.
