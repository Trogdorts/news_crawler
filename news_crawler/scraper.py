import logging
import sys
from pathlib import Path
from newspaper import Source, utils
from newspaper.mthreading import fetch_news
import concurrent.futures
import os
from logging_handler import LoggingHandler
from config_handler import ConfigHandler
from directory_operations import save_article, check_and_create_base_directory
import platform
import random
import yaml
from datetime import datetime, timedelta

class NewsCrawler:
    def __init__(self, config, base_archive_directory, language='en', max_workers=5, sources_per_batch=2, failed_source_threshold=5, failure_time_window_hours=24):
        logging.info(f"Initializing NewsCrawler")
        self.config = config
        self.language = language
        self.first_run = True
        self.base_archive_directory = base_archive_directory
        self.max_workers = max_workers
        self.sources_per_batch = sources_per_batch
        self.failed_source_threshold = failed_source_threshold
        self.failure_time_window = timedelta(hours=failure_time_window_hours)
        self.failure_log = {}
        self.os_type = platform.system()
        self.sources = []
        self.articles = []
        self.cycle = 1

    def set_run(self, run=None):
        if run is True:
            self.first_run = True
        elif run is False:
            self.first_run = False
        else:
            logging.error(f"Invalid run: {run}")
            sys.exit(1)

    def build_source(self, source):
        try:
            source.build()
            logging.debug(f"Built source: {source}")
        except Exception as e:
            logging.error(f"Error building source: {e}")
            self.record_failure(source.url)

    def build_sources(self, max_workers):
        try:
            logging.info("Building sources.")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.build_source, source): source for source in self.sources}
                for future in concurrent.futures.as_completed(futures):
                    source = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Source {source.url} generated an exception: {e}")
            logging.info("Sources built successfully")
        except Exception as e:
            logging.error(f"Error building sources: {e}")

    def crawl_articles(self):
        try:
            logging.info("Crawling articles.")
            self.articles = fetch_news(self.sources, threads=4)
            logging.info("Articles crawled successfully.")
        except Exception as e:
            logging.error(f"Error crawling articles: {e}")

    def extract_information(self):
        try:
            logging.info("Extracting information from articles")
            for source in self.sources:
                logging.debug(f"Processing source: {source.url}")
                for article in source.articles:
                    try:
                        article.download()
                        article.parse()
                        article.nlp()
                        save_article(article, source, self.base_archive_directory, self.os_type)
                    except Exception as e:
                        logging.error(f"Error processing article: {e}")
                        self.record_failure(source.url)
        except Exception as e:
            logging.error(f"Error extracting information: {e}")

    def record_failure(self, url):
        now = datetime.now()
        if url not in self.failure_log:
            self.failure_log[url] = []
        self.failure_log[url].append(now)
        logging.warning(f"Recording failure for {url}")
        self.failure_log[url] = [timestamp for timestamp in self.failure_log[url] if timestamp > now - self.failure_time_window]
        if len(self.failure_log[url]) > self.failed_source_threshold:
            self.mark_source_as_failed(url)
            self.remove_source(url)

    def mark_source_as_failed(self, url):
        logging.warning(f"Marking source {url} as failed due to repeated failures.")
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yml')
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        for source_name, source in config['news_sources'].items():
            if source['base_url'] == url:
                source['failed'] = True
                break
        with open(config_path, 'w') as file:
            yaml.dump(config, file)

    def remove_source(self, url):
        logging.info(f"Removing source {url} from the active sources list.")
        self.sources = [source for source in self.sources if source.url != url]

    def get_source_urls(self):
        try:
            news_sources = self.config.get('news_sources', {})
            base_urls = [source['base_url'] for source in news_sources.values() if not source.get('failed', False)]
            return base_urls
        except Exception as e:
            logging.error(f"Failed to get source URLs: {e}")
            raise

    def run(self, run_once):
        while True:
            logging.info("Starting a new cycle to fetch and process sources.")
            source_urls = self.get_source_urls()
            random.shuffle(source_urls)
            logging.debug(f"Fetched {len(source_urls)} source URLs from configuration.")
            for i in range(0, len(source_urls), self.sources_per_batch):
                batch_urls = source_urls[i:i+self.sources_per_batch]
                logging.info(f"Processing batch URLs: {batch_urls}")
                self.sources = [Source(url, language=self.language) for url in batch_urls]
                for source in self.sources:
                    if self.first_run:
                        logging.info(f"Cleaning memo_cache for {source.url}.")
                        source.clean_memo_cache()
                self.set_run(self.first_run)
                self.build_sources(self.max_workers)
                self.crawl_articles()
                self.extract_information()
            if run_once:
                logging.info("Exiting program after running once.")
                break
            else:
                logging.info(f"Completed cycle {self.cycle}. Preparing for the next cycle.")
                self.cycle += 1
                self.first_run = False

if __name__ == "__main__":
    try:
        config_path = Path(__file__).resolve().parent.parent / 'config.yml'
        handler = ConfigHandler(config_path)
        config = handler.load_config()  # Use the instance method

        if config is None:
            logging.critical("Configuration is None after loading. Exiting.")
            sys.exit(1)

        LoggingHandler.setup_logging(config)

        base_archive_directory = check_and_create_base_directory(config['settings']['base_archive_dir'])
        utils.cache_disk.enabled = False
        run_once = config['settings']['run_once']
        max_workers = config['settings'].get('max_workers', 5)
        sources_per_batch = config['settings'].get('sources_per_batch', 2)
        failed_source_threshold = config['settings'].get('failed_source_threshold', 5)
        failure_time_window_hours = config['settings'].get('failure_time_window_hours', 24)
        language = config['settings'].get('language', 'en')

        crawler = NewsCrawler(config, base_archive_directory, language, max_workers, sources_per_batch, failed_source_threshold, failure_time_window_hours)
        crawler.run(run_once)

    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {e}")
