import yaml
from pathlib import Path
import logging
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

# Define the default configuration file path using Path
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / 'config.yml'

def load_config(config_path):
    """Load the configuration file."""
    try:
        with config_path.open('r') as file:
            config = yaml.safe_load(file)
        logging.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration file: {e}")
        raise

def save_config(config, config_path):
    """Save the configuration file."""
    try:
        with config_path.open('w') as file:
            yaml.safe_dump(config, file, sort_keys=False)
        logging.info(f"Configuration saved to {config_path}")
    except Exception as e:
        logging.error(f"Failed to save configuration file: {e}")
        raise

def remove_duplicates_and_sort(news_sources):
    """Remove duplicates and sort the news sources by their names."""
    unique_news_sources = {source: details for source, details in news_sources.items()}
    sorted_news_sources = dict(sorted(unique_news_sources.items(), key=lambda item: item[0].lower()))
    logging.info("Removed duplicates and sorted news sources alphabetically by name")
    return sorted_news_sources

def main(config_file_path):
    """Main function to process the configuration file."""
    try:
        config = load_config(config_file_path)
        config['news_sources'] = remove_duplicates_and_sort(config['news_sources'])
        save_config(config, config_file_path)
        logging.info(f"Successfully processed the configuration file at {config_file_path}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    # Argument parser for command-line arguments
    parser = argparse.ArgumentParser(description='Process and sort news sources in a configuration file.')
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG_PATH, help='Path to the configuration file (default: %(default)s)')

    args = parser.parse_args()

    main(args.config)
