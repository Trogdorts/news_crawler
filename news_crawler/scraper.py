import logging
from newspaper import Source, utils
from newspaper.mthreading import fetch_news
import concurrent.futures
from datetime import datetime
import os
import re
from unidecode import unidecode
import sys
import yaml
import traceback

# Function to extract base_urls from the already loaded config
def get_source_urls(config):
    try:
        news_sources = config.get('news_sources', {})
        base_urls = [source['base_url'] for source in news_sources.values()]
        return base_urls
    except Exception as e:
        logging.error("Failed to get source URLs: %s", e)
        raise

class ScriptLoggerFilter(logging.Filter):
    def filter(self, record):
        return record.pathname.startswith(os.path.dirname(os.path.abspath(__file__)))

class LoggingHandler:
    @staticmethod
    def setup_logging(config):
        log_level = config.get('logging', {}).get('level', 'DEBUG')
        log_file = config.get('logging', {}).get('file', 'rename_completed_downloads.log')

        try:
            log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', os.path.dirname(log_file))
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            log_file_path = os.path.join(log_dir, os.path.basename(log_file))
            root_logger = logging.getLogger()
            root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - [Line:%(lineno)s] - %(message)s')

            # File handler
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

            # Stream handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            root_logger.addHandler(stream_handler)

            # Add the custom filter to both handlers
            filter = ScriptLoggerFilter()
            file_handler.addFilter(filter)
            stream_handler.addFilter(filter)

            # Suppress logs from the 'newspaper' package
            # logging.getLogger('newspaper').setLevel(logging.CRITICAL)

        except Exception as e:
            raise RuntimeError(f"Error setting up logging: {e}")

def custom_excepthook(exc_type, exc_value, exc_traceback):
    if any(os.path.abspath(__file__) in frame.filename for frame in traceback.extract_tb(exc_traceback)):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

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

def check_and_create_base_directory(base_directory):
    try:
        if not os.path.exists(base_directory):
            local_directory = os.path.join(os.getcwd(), 'archive', 'news')
            os.makedirs(local_directory, exist_ok=True)
            logging.warning(f"Base directory {base_directory} does not exist. Using local directory {local_directory}")
            return local_directory
        else:
            logging.info(f"Base directory {base_directory} exists.")
            return base_directory
    except Exception as e:
        logging.critical(f"Error checking or creating base directory: {e}")
        raise

def clean_filename(title, max_length=255):
    try:
        logging.debug("Cleaning filename for title: %s", title)
        title_ascii = unidecode(title)
        title_clean = re.sub(r'[<>:"/\\|?*\']', '', title_ascii)
        cleaned_title = title_clean[:max_length]
        logging.debug("Cleaned filename: %s", cleaned_title)
        return cleaned_title
    except Exception as e:
        logging.error(f"Error cleaning filename: {e}")
        return "unknown_title"

def extract_year_month_day(timestamp):
    try:
        logging.debug("Extracting year, month, and day from timestamp: %s", timestamp)
        if isinstance(timestamp, str):
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            raise ValueError("Unsupported timestamp format")
        year = dt.year
        month = f"{dt.month:02d}"
        day = f"{dt.day:02d}"
        logging.debug("Extracted year: %s, month: %s, day: %s", year, month, day)
        return year, month, day
    except Exception as e:
        logging.error(f"Error extracting year, month, and day: %s", e)
        return None, None, None

def create_directories(path):
    try:
        os.makedirs(path, exist_ok=True)
        logging.info(f"Directories created successfully or already exist: %s", path)
    except Exception as e:
        logging.error(f"An error occurred while creating directories: %s", e)

def save_article(article, source, base_archive_directory):
    try:
        publish_date = article.publish_date
        title = article.title
        clean_title = clean_filename(title)
        year, month, day = extract_year_month_day(publish_date)

        if year is None:
            logging.debug("Could not extract year from publish date")
            year = 0
        if month is None:
            logging.debug("Could not extract month from publish date")
            month = 0

        if day is not None:
            filename = f"{year:02}-{month:02}-{day:02} {clean_title}.json"
        else:
            filename = f"{year:02}-{month:02} {clean_title}.json"

        save_directory = os.path.join(base_archive_directory, source.brand, str(year), str(month))
        create_directories(save_directory)

        save_path = os.path.join(save_directory, filename)
        json_data = article.to_json()
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(json_data)
                logging.info(f"Article saved to %s", save_path)
        except Exception as e:
            logging.error(f"Error writing to file: %s", e)

    except Exception as e:
        logging.error(f"Failed to save article: %s", e)

class NewsCrawler:
    def __init__(self, source_urls, config=None):
        try:
            logging.info("Initializing NewsCrawler with sources: %s", source_urls)
            self.sources = []
            for url in source_urls:
                source = Source(url, config=config)
                if first_run:
                    source.clean_memo_cache()
                self.sources.append(source)
            self.articles = []
        except Exception as e:
            logging.critical(f"Error initializing NewsCrawler: %s", e)

    def build_source(self, source):
        try:
            source.build()
            logging.debug(f"Built source: {source}")
        except Exception as e:
            logging.error(f"Error building source: {e}")

    def build_sources(self, max_workers):
        try:
            logging.info("Building sources")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.build_source, source): source for source in self.sources}
                for future in concurrent.futures.as_completed(futures):
                    source = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Source %s generated an exception: %s", source.url, e)
            logging.info("Sources built successfully")
        except Exception as e:
            logging.error(f"Error building sources: %s", e)

    def crawl_articles(self):
        try:
            logging.info(f"Crawling articles.")
            self.articles = fetch_news(self.sources, threads=4)
            logging.info(f"Articles crawled successfully.")
        except Exception as e:
            logging.error(f"Error crawling articles.")

    def extract_information(self):
        try:
            logging.info("Extracting information from articles")
            for source in self.sources:
                logging.debug("Processing source: %s", source.url)
                for article in source.articles:
                    try:
                        article.download()
                        article.parse()
                        article.nlp()
                        save_article(article, source, base_archive_directory)
                    except Exception as e:
                        logging.error(f"Error processing article: {e}")
        except Exception as e:
            logging.error(f"Error extracting information: %e")

if __name__ == "__main__":
    # sys.excepthook = custom_excepthook
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yml')
        config = ConfigHandler.load_config(config_path)
        LoggingHandler.setup_logging(config)

        base_archive_directory = check_and_create_base_directory(config['settings']['base_archive_dir'])
        utils.cache_disk.enabled = False
        run_once = config['settings']['run_once']
        max_workers = config['settings'].get('max_workers', 5)  # Default to 5 workers if not specified
        sources_per_batch = config['settings'].get('sources_per_batch', 2)  # Default to 10 sources per batch if not specified
        first_run = True

        while True:
            source_urls = get_source_urls(config)
            for i in range(0, len(source_urls), sources_per_batch):
                batch_urls = source_urls[i:i+sources_per_batch]
                crawler = NewsCrawler(batch_urls)
                crawler.build_sources(max_workers)
                crawler.crawl_articles()
                crawler.extract_information()
            if run_once:
                logging.info(f"Exiting program after running once.")
                break
            else:
                first_run = False
    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.critical(f"Unexpected error in main execution: %s", e)