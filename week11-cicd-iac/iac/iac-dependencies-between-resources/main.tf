provider "aws" {
  region = "eu-central-1"
}

# Base bucket
resource "aws_s3_bucket" "name" {
  bucket = "iac-demo-bucket-998877-1732917235"
}

# File that depends on the bucket 
#  (fixed: aws_s3_object instead of aws_s3_bucket_object)
resource "aws_s3_object" "readme" {
  bucket  = aws_s3_bucket.name.bucket  # use .bucket for name
  key     = "README.txt"               # object name in S3
  content = "This file was provisioned by Terraform"
}