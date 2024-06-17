import logging
from newspaper import Source, utils
from newspaper.mthreading import fetch_news
import concurrent.futures
import os
from logging_handler import LoggingHandler
from config_handler import ConfigHandler
from directory_operations import save_article, check_and_create_base_directory
import platform
import random

# Function to extract base_urls from the already loaded config
def get_source_urls(config):
    try:
        news_sources = config.get('news_sources', {})
        base_urls = [source['base_url'] for source in news_sources.values()]
        return base_urls
    except Exception as e:
        logging.error(f"Failed to get source URLs: {e}")
        raise

class NewsCrawler:
    def __init__(self, source_urls, config=None):
        try:
            logging.info(f"Initializing NewsCrawler with sources: {source_urls}")
            self.sources = []
            for url in source_urls:
                source = Source(url, config=config)
                if first_run:
                    source.clean_memo_cache()
                self.sources.append(source)
            self.articles = []
        except Exception as e:
            logging.critical(f"Error initializing NewsCrawler: {e}")

    def build_source(self, source):
        try:
            source.build()
            logging.debug(f"Built source: {source}")
        except Exception as e:
            logging.error(f"Error building source: {e}")

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

    def extract_information(self, os_type):
        try:
            logging.info("Extracting information from articles")
            for source in self.sources:
                logging.debug(f"Processing source: {source.url}")
                for article in source.articles:
                    try:
                        article.download()
                        article.parse()
                        article.nlp()
                        save_article(article, source, base_archive_directory, os_type)
                    except Exception as e:
                        logging.error(f"Error processing article: {e}")
        except Exception as e:
            logging.error(f"Error extracting information: {e}")

if __name__ == "__main__":
    #TODO - batch save files to cut down on write speeds
    #TODO - create rotating log files
    #TODO - add ability to compress folders
    #TODO - add ability to scrape wayback
    #TODO - add in the ability to send web updates to cron scheduler
    #TODO - add a function that will generate an empty config file
    #TODO - add a language filter
    #TODO - add the ability to pass in a different config file

    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yml')
        config = ConfigHandler.load_config(config_path)
        LoggingHandler.setup_logging(config)

        base_archive_directory = check_and_create_base_directory(config['settings']['base_archive_dir'])
        utils.cache_disk.enabled = False
        run_once = config['settings']['run_once']
        max_workers = config['settings'].get('max_workers', 5)  # Default to 5 workers if not specified
        sources_per_batch = config['settings'].get('sources_per_batch', 2)  # Default to 2 sources per batch if not specified
        first_run = True
        os_type = platform.system()

        while True:
            logging.info("Starting a new cycle to fetch and process sources.")
            source_urls = get_source_urls(config)
            random.shuffle(source_urls)
            logging.debug(f"Fetched {len(source_urls)} source URLs from configuration.")
            for i in range(0, len(source_urls), sources_per_batch):
                batch_urls = source_urls[i:i+sources_per_batch]
                logging.info(f"Processing batch URLs: {batch_urls}")
                crawler = NewsCrawler(batch_urls)
                crawler.build_sources(max_workers)
                crawler.crawl_articles()
                crawler.extract_information(os_type=os_type)
            if run_once:
                logging.info("Exiting program after running once.")
                break
            else:
                logging.info("Completed current cycle. Preparing for the next cycle.")
                first_run = False
    except KeyboardInterrupt:
        logging.info("Script interrupted by user.")
    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {e}")