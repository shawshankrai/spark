# env

This folder contains environment-specific subfolders for Terraform configuration.

- `dev/` for development environment
- `prod/` for production environment

Each subfolder contains:
- `*.tfvars` for input variables
- `*-backend-config.hcl` for backend state config

Usage example:
```
terraform init -backend-config=env/dev/dev-backend-config.hcl
terraform apply -var-file=env/dev/dev.tfvars

terraform init -backend-config=env/prod/prod-backend-config.hcl
terraform apply -var-file=env/prod/prod.tfvars
``` 