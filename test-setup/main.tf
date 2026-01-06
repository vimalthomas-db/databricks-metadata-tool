# =============================================================================
# Databricks Metadata Tool - Test Environment
# =============================================================================
# This Terraform creates:
# 1. Resource Group
# 2. Admin Workspace (Unity Catalog)
# 3. Analytical Workspace (Unity Catalog) 
# 4. Legacy Workspace (HMS only - no UC)
# 5. Unity Catalog Metastore
# =============================================================================

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# =============================================================================
# Variables
# =============================================================================

variable "location" {
  description = "Azure region"
  default     = "eastus2"
}

variable "prefix" {
  description = "Prefix for all resources"
  default     = "dbmeta-test"
}

variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

# =============================================================================
# Provider Configuration
# =============================================================================

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# =============================================================================
# Resource Group
# =============================================================================

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.prefix}"
  location = var.location

  tags = {
    environment = "test"
    project     = "databricks-metadata-tool"
  }
}

# =============================================================================
# Workspaces
# =============================================================================

# Admin Workspace (Unity Catalog enabled)
resource "azurerm_databricks_workspace" "admin" {
  name                          = "dbw-${var.prefix}-admin"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  sku                           = "premium"  # Required for Unity Catalog
  managed_resource_group_name   = "mrg-${var.prefix}-admin"  # Unique managed RG

  tags = {
    environment = "test"
    role        = "admin"
  }
}

# Analytical Workspace (Unity Catalog enabled - will have test tables)
resource "azurerm_databricks_workspace" "analytics" {
  name                          = "dbw-${var.prefix}-analytics"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  sku                           = "premium"  # Required for Unity Catalog
  managed_resource_group_name   = "mrg-${var.prefix}-analytics"  # Unique managed RG

  depends_on = [azurerm_databricks_workspace.admin]  # Sequential creation

  tags = {
    environment = "test"
    role        = "analytics"
  }
}

# Legacy Workspace (NO Unity Catalog - HMS only)
resource "azurerm_databricks_workspace" "legacy" {
  name                          = "dbw-${var.prefix}-legacy"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  sku                           = "standard"  # Standard SKU = no UC
  managed_resource_group_name   = "mrg-${var.prefix}-legacy"  # Unique managed RG

  depends_on = [azurerm_databricks_workspace.analytics]  # Sequential creation

  tags = {
    environment = "test"
    role        = "legacy-hms"
  }
}

# =============================================================================
# Storage Account for Unity Catalog Metastore
# =============================================================================

resource "azurerm_storage_account" "metastore" {
  name                     = "st${replace(var.prefix, "-", "")}uc"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # Required for ADLS Gen2

  tags = {
    environment = "test"
    purpose     = "unity-catalog-metastore"
  }
}

resource "azurerm_storage_container" "metastore" {
  name                  = "metastore"
  storage_account_name  = azurerm_storage_account.metastore.name
  container_access_type = "private"
}

# =============================================================================
# Outputs
# =============================================================================

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "admin_workspace_url" {
  value = azurerm_databricks_workspace.admin.workspace_url
}

output "admin_workspace_id" {
  value = azurerm_databricks_workspace.admin.workspace_id
}

output "analytics_workspace_url" {
  value = azurerm_databricks_workspace.analytics.workspace_url
}

output "analytics_workspace_id" {
  value = azurerm_databricks_workspace.analytics.workspace_id
}

output "legacy_workspace_url" {
  value = azurerm_databricks_workspace.legacy.workspace_url
}

output "legacy_workspace_id" {
  value = azurerm_databricks_workspace.legacy.workspace_id
}

output "metastore_storage_url" {
  value = "abfss://${azurerm_storage_container.metastore.name}@${azurerm_storage_account.metastore.name}.dfs.core.windows.net/"
}

