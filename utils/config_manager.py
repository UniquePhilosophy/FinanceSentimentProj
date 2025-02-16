import yaml

class ConfigManager:
    _config = None

    @classmethod
    def load_config(cls, config_file='config/config.yml'):
        if cls._config is None:
            with open(config_file, 'r') as file:
                cls._config = yaml.safe_load(file)
        return cls._config

    @classmethod
    def get_config(cls):
        if cls._config is None:
            raise Exception("Configuration not loaded. Call 'load_config' first.")
        return cls._config
