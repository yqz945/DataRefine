import os
from typing import Dict

def resolve_env_vars(config: Dict) -> Dict:
    """解析配置中的环境变量"""
    resolved_config = {}
    for key, value in config.items():
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            resolved_config[key] = os.getenv(env_var)
        elif isinstance(value, dict):
            resolved_config[key] = resolve_env_vars(value)
        else:
            resolved_config[key] = value
    return resolved_config 