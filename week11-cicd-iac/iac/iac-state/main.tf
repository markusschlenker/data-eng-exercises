provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "demo" {
  bucket = "iac-state-bucket-12345-8436985634"

  tags = {
    Name = "iac-demo"
    Env  = "dev"
  }
}
