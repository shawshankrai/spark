# variables.tf - Input variables for Terraform

variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket for Glue scripts"
  type        = string
}

variable "glue_job_name" {
  description = "Name of the AWS Glue job"
  type        = string
  default     = "scala-glue-job"
} 