import yaml
import logging
from datetime import datetime, date
import sys
from pathlib import Path
import argparse

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('script.log'),
        logging.StreamHandler()
    ]
)

class ConfigHandler:
    def __init__(self, config_path):
        self.config_path = Path(config_path)
        self.config = None

    def load_config(self):
        """Load the configuration file."""
        try:
            with self.config_path.open('r') as file:
                self.config = yaml.safe_load(file)
            self.validate_config(self.config)
            logging.info(f"Loaded configuration from {self.config_path}")
            return self.config  # Ensure the method returns the loaded config
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
            elif 'date' in key.lower() and isinstance(value, str):
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

    def save_config(self):
        """Save the configuration file, ensuring news_sources is the last key."""
        try:
            if 'news_sources' in self.config:
                news_sources = self.config.pop('news_sources')
                with self.config_path.open('w') as file:
                    yaml.safe_dump(self.config, file, sort_keys=False)
                    yaml.safe_dump({'news_sources': news_sources}, file, sort_keys=False)
            else:
                with self.config_path.open('w') as file:
                    yaml.safe_dump(self.config, file, sort_keys=False)
            logging.info(f"Configuration saved to {self.config_path}")
        except Exception as e:
            logging.error(f"Failed to save configuration file: {e}")
            raise

    def remove_duplicates_and_sort(self):
        """Remove duplicates and sort the news sources by their names."""
        if 'news_sources' in self.config:
            news_sources = self.config['news_sources']
            unique_news_sources = {source: details for source, details in news_sources.items()}
            sorted_news_sources = dict(sorted(unique_news_sources.items(), key=lambda item: item[0].lower()))
            self.config['news_sources'] = sorted_news_sources
            logging.info("Removed duplicates and sorted news sources alphabetically by name")

    def process_config(self):
        """Process the configuration file."""
        try:
            self.load_config()
            self.remove_duplicates_and_sort()
            self.save_config()
            logging.info(f"Successfully processed the configuration file at {self.config_path}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

def main(config_file_path):
    """Main function to process the configuration file."""
    handler = ConfigHandler(config_file_path)
    handler.process_config()

if __name__ == "__main__":
    # Argument parser for command-line arguments
    parser = argparse.ArgumentParser(description='Process and sort news sources in a configuration file.')
    parser.add_argument('--config', type=str, default=str(Path(__file__).resolve().parent.parent / 'config.yml'), help='Path to the configuration file (default: %(default)s)')

    args = parser.parse_args()

    main(args.config)
