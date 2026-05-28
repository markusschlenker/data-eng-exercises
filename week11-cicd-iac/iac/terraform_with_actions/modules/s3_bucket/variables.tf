variable "bucket_name" {
  description = "Name of the S3 bucket to create"
  type        = string
}

variable "enable_versioning" {
  description = "Enable versioning on the bucket"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Map of tags to attach to the bucket"
  type        = map(string)
  default     = {}
}