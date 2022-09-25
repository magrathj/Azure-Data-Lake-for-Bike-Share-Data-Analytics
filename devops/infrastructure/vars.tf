variable "resource_group_name" {
    description = "rsg for the virtual machine's name which will be created"
    default     = "udacity-data-lake"
}

variable "location" {
  description = "azure region where resources will be located .e.g. northeurope"
  default     = "UK South"
}
