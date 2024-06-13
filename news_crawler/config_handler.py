import yaml
import logging
from datetime import datetime
class ConfigHandler:
    @staticmethod
    def load_config(config_path):
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            ConfigHandler.validate_config(config)
            return config
        except Exception as e:
            logging.error(f"Error loading configuration file: {e}")
            raise

    @staticmethod
    def validate_config(config, parent_key=''):
        for key, value in config.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(value, dict):
                ConfigHandler.validate_config(value, full_key)
            elif value == '':
                config[key] = None
            elif 'date' in key.lower() and value:
                try:
                    config[key] = ConfigHandler.validate_date(value)
                except ValueError as e:
                    logging.error(f"Invalid date format for key: {full_key}, value: {value}")
                    sys.exit(1)

    @staticmethod
    def validate_date(date_str):
        date_formats = [
            '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y',
            '%Y/%m/%d', '%B %d, %Y', '%d %B %Y'
        ]

        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue
        raise ValueError(f"Invalid date format: {date_str}")