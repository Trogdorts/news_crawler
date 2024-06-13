import os
import logging
import sys
import json
import re
from datetime import datetime
from dateutil.parser import parse
from news_crawler.config_handler import ConfigHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def walk_and_rename_json_files(directory):
    # Regular expression to match filenames starting with year-month pattern
    pattern = re.compile(r'^\d{4}-\d{2} ')

    for root, dirs, files in os.walk(directory):
        logging.debug(f"Walking through directory: {root}")
        for file in files:
            logging.debug(f"Found file: {file}")
            if file.endswith(".json") and pattern.match(file):
                old_path = os.path.join(root, file)
                logging.debug(f"Processing JSON file: {old_path}")
                try:
                    with open(old_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        logging.debug(f"Loaded JSON data from {file}: {data}")

                    # Extract publish_date and convert to the new filename
                    if 'publish_date' in data:
                        publish_date_str = data['publish_date']
                        publish_date = parse(publish_date_str)
                        new_date_str = publish_date.strftime("%Y-%m-%d")
                        logging.debug(f"Extracted publish_date: {publish_date_str}, formatted as: {new_date_str}")

                        # Extract the title part from the old filename
                        title = file.split(' ', 1)[1]
                        new_filename = f"{new_date_str} {title}"
                        new_path = os.path.join(root, new_filename)

                        # Rename the file
                        os.rename(old_path, new_path)
                        logging.info(f"Renamed '{old_path}' to '{new_path}'")

                    else:
                        logging.warning(f"No 'publish_date' field in JSON data: {file}")
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON from file: {file}", exc_info=True)
                except FileNotFoundError:
                    logging.error(f"File not found: {old_path}", exc_info=True)
                except Exception as e:
                    logging.error(f"Failed to process '{old_path}': {e}", exc_info=True)
            else:
                logging.debug(f"File '{file}' does not match the criteria for processing")

if __name__ == "__main__":
    logging.info(f"Starting rename.")
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yml')
        config = ConfigHandler.load_config(config_path)
        base_archive_directory = config['settings']['base_archive_dir']

        if not os.path.exists(base_archive_directory):
            logging.error('Base archive directory does not exist')
            sys.exit(1)

        walk_and_rename_json_files(base_archive_directory)
        logging.info(f"Finished rename.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")

    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {e}")
