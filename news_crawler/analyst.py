import os
import logging
from logging_handler import  LoggingHandler
from config_handler import ConfigHandler
import time
from pathlib import Path
class Analyst:

    def get_articles_by_date(self,base_dir, date):
        _start_time = time.time()
        year, month, day = date.split('-')
        matched_files = []
        for source in os.listdir(base_dir):
            source_path = os.path.join(base_dir, source, year, month)
            if os.path.isdir(source_path):
                for file in os.listdir(source_path):
                    if file.startswith(date):
                        matched_files.append(os.path.join(source_path, file))
        _end_time = time.time()
        logging.info(f"Analyst: get_articles_by_date: {_end_time - _start_time} seconds, {len(matched_files)} articles")
        return matched_files


if __name__ == '__main__':
    config_path = Path(__file__).resolve().parent.parent / 'config.yml'
    handler = ConfigHandler(config_path)
    config = handler.load_config()  # Use the instance method
    LoggingHandler.setup_logging(config)
    logging.info

    base_archive_directory = r'Z:\data\archive\news'
    analyst = Analyst()
    search_date = '2024-06-18'
    matched_files = analyst.get_articles_by_date(base_archive_directory, search_date)

    for match in matched_files:
        print(match)