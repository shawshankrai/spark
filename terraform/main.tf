# main.tf - AWS Glue job, IAM role, S3 bucket, and JAR upload

# AWS provider configuration
provider "aws" {
  region = var.aws_region
}

# IAM policy document for Glue to assume the role
# Allows Glue service to use this role
data "aws_iam_policy_document" "glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# IAM role for Glue job execution
resource "aws_iam_role" "glue_role" {
  name = "glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy.json
}

# Attach AWS managed Glue service role policy to the IAM role
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 bucket to store Glue scripts and JARs
resource "aws_s3_bucket" "glue_scripts" {
  bucket = var.s3_bucket_name
}

# Upload the Spark assembly JAR to S3 for Glue to use
resource "aws_s3_object" "glue_jar" {
  bucket = aws_s3_bucket.glue_scripts.bucket
  key    = "scripts/spark-project-assembly-0.1.0.jar"
  source = "../target/scala-2.12/spark-project-assembly-0.1.0.jar"
  etag   = filemd5("../target/scala-2.12/spark-project-assembly-0.1.0.jar")
  content_type = "application/java-archive"
}

# AWS Glue job definition
resource "aws_glue_job" "spark_job" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.glue_jar.key}"
    python_version  = "3"
  }
  glue_version      = "4.0"
  max_capacity      = 2
  number_of_workers = 2
  worker_type       = "G.1X"
} 