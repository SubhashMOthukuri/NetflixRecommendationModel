"""
Configuration Loader - Production ML Pipeline
Loads YAML configuration files (Netflix standards)
Used by all pipeline components to read configs
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from libs.exceptions import PipelineException


class ConfigLoadError(PipelineException):
    """Raised when config file cannot be loaded"""
    pass


def get_project_root() -> Path:
    """
    Get project root directory
    
    Returns:
        Path to project root
    """
    # Go up from libs/ to project root
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    return project_root


def load_config(config_file: str, environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_file: Name of config file (e.g., "kafka.yaml")
        environment: Optional environment name (dev/staging/prod)
        
    Returns:
        Dictionary with configuration
        
    Raises:
        ConfigLoadError: If config file cannot be loaded
        
    Example:
        config = load_config("kafka.yaml")
        kafka_url = config["broker"]["bootstrap_servers"]
    """
    project_root = get_project_root()
    config_path = project_root / "config" / config_file
    
    # Check if file exists
    if not config_path.exists():
        raise ConfigLoadError(f"Config file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # If environment specified, merge with environment configs
        if environment:
            env_config = load_environment_config(environment)
            if env_config:
                config = merge_configs(config, env_config)
        
        return config or {}
    
    except yaml.YAMLError as e:
        raise ConfigLoadError(f"Error parsing YAML file {config_file}: {str(e)}")
    except Exception as e:
        raise ConfigLoadError(f"Error loading config file {config_file}: {str(e)}")


def load_environment_config(environment: str) -> Optional[Dict[str, Any]]:
    """
    Load environment-specific configuration
    
    Args:
        environment: Environment name (dev/staging/prod)
        
    Returns:
        Environment config dictionary or None
    """
    try:
        env_config = load_config("environments.yaml")
        return env_config.get("environments", {}).get(environment)
    except Exception:
        return None


def merge_configs(base_config: Dict, env_config: Dict) -> Dict:
    """
    Merge environment config into base config
    
    Args:
        base_config: Base configuration
        env_config: Environment-specific configuration
        
    Returns:
        Merged configuration
    """
    merged = base_config.copy()
    
    for key, value in env_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    
    return merged


def get_kafka_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Get Kafka configuration
    
    Args:
        environment: Optional environment name
        
    Returns:
        Kafka configuration dictionary
    """
    return load_config("kafka.yaml", environment)


def get_config_value(config: Dict, key_path: str, default: Any = None) -> Any:
    """
    Get nested config value using dot notation
    
    Args:
        config: Configuration dictionary
        key_path: Dot-separated path (e.g., "broker.bootstrap_servers")
        default: Default value if key not found
        
    Returns:
        Config value or default
        
    Example:
        kafka_url = get_config_value(config, "broker.bootstrap_servers")
    """
    keys = key_path.split('.')
    value = config
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    
    return value

