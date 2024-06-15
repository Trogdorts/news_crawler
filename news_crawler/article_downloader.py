import logging
import os
import re
from datetime import datetime
from newspaper import Article
from unidecode import unidecode
import platform
import concurrent.futures
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ArticleDownloader:
    def __init__(self, config):
        self.urls_file_path = config['urls_file_path']
        self.base_archive_directory = config['base_archive_directory']
        self.source_name = config['source_name']
        self.max_workers = config.get('max_workers', 5)
        self.os_type = platform.system()
        self.urls = self.read_urls()

    # Function to read URLs from a file
    def read_urls(self):
        try:
            with open(self.urls_file_path, 'r') as file:
                urls = file.read().splitlines()
                return urls
        except Exception as e:
            logging.error(f"Error reading URLs from file: {e}")
            return []

    # Function to clean filename
    def clean_filename(self, title):
        max_length = 255
        windows_restricted_chars = r'[<>:"/\\|?*]'
        linux_restricted_chars = r'[<>:"/\\|?*\']'

        try:
            title_ascii = unidecode(title)
            if self.os_type.lower() == 'linux':
                title_clean = re.sub(linux_restricted_chars, '', title_ascii)
            else:
                title_clean = re.sub(windows_restricted_chars, '', title_ascii)
                title_clean = title_clean.rstrip('. ')
                title_clean = title_clean[:max_length]
            return title_clean
        except Exception as e:
            logging.error(f"Error cleaning filename: {e}")
            return "unknown_title"

    # Function to extract year, month, and day from publish date
    def extract_year_month_day(self, timestamp):
        try:
            if isinstance(timestamp, str):
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            elif isinstance(timestamp, datetime):
                dt = timestamp
            else:
                raise ValueError("Unsupported timestamp format")
            return dt.year, f"{dt.month:02d}", f"{dt.day:02d}"
        except Exception as e:
            logging.error(f"Error extracting year, month, and day: {e}")
            return None, None, None

    # Function to save article as JSON
    def save_article(self, article):
        try:
            publish_date = article.publish_date
            title = article.title
            clean_title = self.clean_filename(title)
            year, month, day = self.extract_year_month_day(publish_date)

            if year is None:
                year = 0
            if month is None:
                month = 0
            if day is not None:
                filename = f"{year:02}-{month:02}-{day:02} {clean_title}.json"
            else:
                filename = f"{year:02}-{month:02} {clean_title}.json"

            save_directory = os.path.join(self.base_archive_directory, self.source_name, str(year), str(month))
            os.makedirs(save_directory, exist_ok=True)

            article_json = {
                'title': article.title,
                'text': article.text,
                'publish_date': article.publish_date.isoformat() if article.publish_date else None,
                'authors': article.authors,
                'top_image': article.top_image,
                'movies': article.movies,
                'keywords': article.keywords,
                'summary': article.summary,
                'meta_site_name': article.meta_site_name
            }
            save_path = os.path.join(save_directory, filename)
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(article_json, indent=4))
                logging.info(f"Article saved to {save_path}")
        except Exception as e:
            logging.error(f"Failed to save article: {e}")

    # Function to process a single URL
    def process_url(self, url):
        try:
            logging.info(f"Processing URL: {url}")
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            self.save_article(article)
        except Exception as e:
            logging.error(f"Error downloading article from {url}: {e}")

    # Main function to download articles from URLs concurrently
    def download_articles(self):
        if self.urls:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self.process_url, url) for url in self.urls]
                concurrent.futures.wait(futures)
        else:
            logging.error("No URLs to process.")

if __name__ == "__main__":



    config = {
        "urls_file_path": "../scripts/urls.txt",  # Path to the file containing URLs
        "base_archive_directory": "/mnt/nas/data/archive/news",  # Base directory to save the downloaded articles
        "source_name": "washingtonexaminer",  # Manually set source folder name
        "max_workers": 5  # Number of threads to use for downloading articles
    }

    downloader = ArticleDownloader(config)
    downloader.download_articles()
