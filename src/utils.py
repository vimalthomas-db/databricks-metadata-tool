import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger('databricks_metadata_tool')
    logger.setLevel(log_level)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    # Load .env from the project root (same directory as config.yaml)
    project_root = Path(config_path).parent if os.path.dirname(config_path) else Path.cwd()
    env_path = project_root / '.env'
    load_dotenv(dotenv_path=env_path)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please copy config.yaml.example to config.yaml and update with your settings."
        )
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Databricks Account config
    if 'databricks' not in config:
        config['databricks'] = {}
    config['databricks']['account_id'] = os.getenv('DATABRICKS_ACCOUNT_ID') or config['databricks'].get('account_id')
    config['databricks']['account_host'] = config['databricks'].get('account_host', 'https://accounts.azuredatabricks.net')
    
    # Azure config
    if 'azure' in config:
        config['azure']['subscription_id'] = os.getenv('AZURE_SUBSCRIPTION_ID') or config['azure'].get('subscription_id')
        config['azure']['tenant_id'] = os.getenv('AZURE_TENANT_ID') or config['azure'].get('tenant_id')
        config['azure']['client_id'] = os.getenv('AZURE_CLIENT_ID') or config['azure'].get('client_id')
        config['azure']['client_secret'] = os.getenv('AZURE_CLIENT_SECRET') or config['azure'].get('client_secret')
        
        env_rg = os.getenv('AZURE_RESOURCE_GROUP')
        config_rg = config['azure'].get('resource_group')
        config['azure']['resource_group'] = env_rg if env_rg else config_rg

    if 'collection' in config:
        config['collection']['warehouse_id'] = os.getenv('DATABRICKS_WAREHOUSE_ID', config['collection'].get('warehouse_id'))
    
    # Discovery mode: 'account' or 'azure'
    config['discovery_mode'] = config.get('discovery_mode', 'azure')
    
    return config


def format_bytes(size_bytes: Optional[int]) -> str:
    if size_bytes is None:
        return "Unknown"
    
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"


def parse_table_properties(properties: Dict[str, str], data_source_format: Optional[str] = None, table_type: Optional[str] = None) -> Dict[str, Any]:
    parsed = {
        'is_delta': False,
        'is_iceberg': False,
        'is_uniform': False,
        'table_format': 'UNKNOWN'
    }
    
    # Use SDK data_source_format as primary source
    if data_source_format:
        parsed['table_format'] = data_source_format.upper()
        parsed['is_delta'] = data_source_format.upper() == 'DELTA'
        parsed['is_iceberg'] = data_source_format.upper() in ['ICEBERG', 'ICEBERG_TABLE']
    elif table_type and table_type == 'VIEW':
        parsed['table_format'] = 'VIEW'
    else:
        # Fallback: Try to detect from properties
        if properties:
            if properties.get('delta.minReaderVersion') or properties.get('delta.minWriterVersion'):
                parsed['is_delta'] = True
                parsed['table_format'] = 'DELTA'
            elif 'iceberg' in properties.get('format', '').lower():
                parsed['is_iceberg'] = True
                parsed['table_format'] = 'ICEBERG'
    
    # Check for UniForm (Delta + Iceberg compatibility) - requires properties
    if properties:
        if properties.get('delta.universalFormat.enabledFormats') or \
           properties.get('delta.enableIcebergCompatV2'):
            parsed['is_uniform'] = True
            if parsed['is_delta']:
                parsed['table_format'] = 'UNIFORM'
    
    return parsed


def extract_storage_info(storage_url: str) -> Dict[str, Optional[str]]:
    import re
    
    result = {
        'storage_account': None,
        'storage_type': None
    }
    
    if not storage_url:
        return result
    
    abfss_match = re.match(r'^abfss?://[^@]+@([^.]+)\.dfs\.core\.windows\.net', storage_url)
    if abfss_match:
        result['storage_account'] = abfss_match.group(1)
        result['storage_type'] = 'abfss'
        return result
    
    wasbs_match = re.match(r'^wasbs?://[^@]+@([^.]+)\.blob\.core\.windows\.net', storage_url)
    if wasbs_match:
        result['storage_account'] = wasbs_match.group(1)
        result['storage_type'] = 'wasbs'
        return result
    
    s3_match = re.match(r'^s3a?://([^/]+)', storage_url)
    if s3_match:
        result['storage_account'] = s3_match.group(1)
        result['storage_type'] = 's3'
        return result
    
    gcs_match = re.match(r'^gs://([^/]+)', storage_url)
    if gcs_match:
        result['storage_account'] = gcs_match.group(1)
        result['storage_type'] = 'gcs'
        return result
    
    return result