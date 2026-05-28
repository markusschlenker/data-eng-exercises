provider "aws" {
  region = "eu-central-1"
}

resource "aws_s3_bucket" "logs" {
  bucket = "iac-logs-bucket-12345-15134651346"
}

resource "aws_s3_bucket" "static" {
  bucket = "iac-static-bucket-12345-97696723465"
}