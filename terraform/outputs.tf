# outputs.tf - Outputs for Terraform

output "s3_bucket_name" {
  description = "S3 bucket for Glue scripts"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "glue_job_name" {
  description = "Name of the AWS Glue job"
  value       = aws_glue_job.spark_job.name
}

output "glue_role_arn" {
  description = "IAM Role ARN for Glue job"
  value       = aws_iam_role.glue_role.arn
} 