provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "main" {
  name     = "${var.resource_group_name}-rg"
  location = var.location

  tags = {
    udacity = "${var.resource_group_name}-data-lake"
    Environment = "Dev"
  }

}


resource "azurerm_databricks_workspace" "main" {
  name                = "databricks-data-lake"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"

  tags = {
    udacity = "${var.resource_group_name}-data-lake"
    Environment = "Dev"
  }
}