terraform {
  backend "s3" {
    bucket         = "terraform-with-actions-bucket-demo987594"
    key            = "env/dev/terraform.tfstate"
    region         = "eu-central-1"
    #_dynamodb_table = "terraform-with-actions-locks-1234"
    encrypt        = true
    use_lockfile = true
  }
}


provider "aws" {
  region = "eu-central-1"
}

resource "random_id" "suffix" {
  byte_length = 4
}

module "app_bucket" {
  source           = "../../modules/s3_bucket"
  bucket_name      = "myapp-${terraform.workspace}-bucket-${random_id.suffix.hex}"
  enable_versioning = false
  tags = { env = terraform.workspace }
}

