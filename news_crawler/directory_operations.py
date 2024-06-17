import logging
import os
import re
from unidecode import unidecode
from datetime import datetime

def extract_year_month_day(timestamp):
    try:
        logging.debug(f"Extracting year, month, and day from timestamp: {timestamp}")
        if isinstance(timestamp, str):
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            raise ValueError("Unsupported timestamp format")
        year = dt.year
        month = f"{dt.month:02d}"
        day = f"{dt.day:02d}"
        logging.debug(f"Extracted year: {year}, month: {month}, day: {day}")
        return year, month, day
    except Exception as e:
        logging.error(f"Error extracting year, month, and day: {e}")
        return None, None, None
def check_and_create_base_directory(base_directory):
    try:
        if not os.path.exists(base_directory):
            local_directory = os.path.join(os.getcwd(), 'archive', 'news')
            os.makedirs(local_directory, exist_ok=True)
            logging.warning(f"Base directory {base_directory} does not exist. Using local directory {local_directory}")
            return local_directory
        else:
            logging.debug(f"Base directory {base_directory} exists.")
            return base_directory
    except Exception as e:
        logging.critical(f"Error checking or creating base directory: {e}")
        raise

def clean_filename(title, os_type='Windows'):
    max_length = 255
    windows_restricted_chars = r'[<>:"/\\|?*]'  # Restricted characters for Windows
    linux_restricted_chars = r'[<>:"/\\|?*\']'  # Restricted characters for Linux

    try:
        logging.debug(f"Cleaning filename for title: {title}")
        title_ascii = unidecode(title)

        if os_type.lower() == 'linux':
            title_clean = re.sub(linux_restricted_chars, '', title_ascii)
        else:
            title_clean = re.sub(windows_restricted_chars, '', title_ascii)
            # Remove trailing dots and spaces for Windows
            title_clean = title_clean.rstrip('. ')
            # Apply max length restriction for Windows
            title_clean = title_clean[:max_length]

        logging.info(f"Cleaned filename: {title_clean} from: {title}")
        return title_clean
    except Exception as e:
        logging.error(f"Error cleaning filename: {e}")
        return "unknown_title"


def create_directories(path):
    logging.debug(f"Attempting to create directory: {path}")
    try:
        if os.path.exists(path):
            logging.debug(f"Directory already exists: {path}")
        else:
            os.makedirs(path, exist_ok=True)
            logging.info(f"Directory created successfully: {path}")
    except Exception as e:
        logging.error(f"An error occurred while creating directory: {path} - {e}")
        raise


def save_article(article, source, base_archive_directory, os_type):
    try:
        publish_date = article.publish_date
        title = article.title
        clean_title = clean_filename(title, os_type)
        year, month, day = extract_year_month_day(publish_date)

        if year is None:
            logging.warning("Could not extract year from publish date")
            year = 0
        if month is None:
            logging.warning("Could not extract month from publish date")
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
                logging.info(f"Article saved to {save_path}")
        except Exception as e:
            logging.error(f"Error writing to file: {e}")

    except Exception as e:
        logging.error(f"Failed to save article: {e}")