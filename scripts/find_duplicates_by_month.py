import os
import logging
import sys
import json
from collections import defaultdict
from news_crawler.config_handler import ConfigHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_files_by_folder(directory):
    folder_url_to_files = defaultdict(lambda: defaultdict(list))

    for root, dirs, files in os.walk(directory):
        logging.debug(f"Walking through directory: {root}")
        folder_name = os.path.basename(root)
        for file in files:
            logging.debug(f"Found file: {file}")
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                logging.debug(f"Processing JSON file: {file_path}")
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        logging.debug(f"Loaded JSON data from {file}: {data}")

                    if 'url' in data:
                        url = data['url']
                        folder_url_to_files[folder_name][url].append(file_path)
                    else:
                        logging.warning(f"No 'url' field in JSON data: {file}")

                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON from file: {file}", exc_info=True)
                except FileNotFoundError:
                    logging.error(f"File not found: {file_path}", exc_info=True)
                except Exception as e:
                    logging.error(f"Failed to process '{file_path}': {e}", exc_info=True)
            else:
                logging.debug(f"File '{file}' does not match the criteria for processing")

    return folder_url_to_files


def save_duplicates_by_folder(folder_url_to_files, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        for folder, url_to_files in folder_url_to_files.items():
            duplicate_found = False
            folder_output = f"Folder: {folder}\n"
            for url, paths in url_to_files.items():
                if len(paths) > 1:
                    duplicate_found = True
                    folder_output += f"  URL: {url}\n"
                    for path in paths:
                        folder_output += f"    {path}\n"
            if duplicate_found:
                f.write(folder_output + "\n")
    logging.info(f"Duplicates saved to {output_file}")


if __name__ == "__main__":
    logging.info(f"Starting find duplicates by folder.")
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yml')
        config = ConfigHandler.load_config(config_path)
        base_archive_directory = config['settings']['base_archive_dir']

        if not os.path.exists(base_archive_directory):
            logging.error('Base archive directory does not exist')
            sys.exit(1)

        folder_url_to_files = process_files_by_folder(base_archive_directory)
        save_duplicates_by_folder(folder_url_to_files, 'duplicates.txt')
        logging.info(f"Finished processing.")

    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")

    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {e}")
