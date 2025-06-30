bucket         = "my-prod-terraform-state-bucket"
key            = "spark/glue/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "prod-terraform-lock-table"
encrypt        = true 