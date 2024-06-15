import requests
from bs4 import BeautifulSoup
import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime


# Function to get the links from a single page
def get_links_from_page(soup):
    links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if (
                '/news/' in href and
                not '/section/' in href and
                not href.endswith('#respond') and
                href.startswith('https://www.washingtonexaminer.com/')
        ):
            if href not in links:
                links.append(href)
    return links


# Base URL structure of the archive pages
base_url = 'https://www.washingtonexaminer.com/tag/tws-archive/page/'
progress_file = 'last_page.txt'


# Function to read the last successful page number from the file
def read_last_page():
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            try:
                return int(file.read().strip())
            except ValueError:
                return 1
    return 1


# Function to save the last successful page number to the file
def save_last_page(page_number):
    with open(progress_file, 'w') as file:
        file.write(str(page_number))


# Function to scrape a single page
def scrape_page(page_number):
    current_url = f"{base_url}{page_number}/"
    try:
        print(f'Scraping page: {current_url}')
        response = requests.get(current_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = get_links_from_page(soup)
        return page_number, links
    except requests.exceptions.RequestException as e:
        print(f'Error fetching {current_url}: {e}')
        return page_number, []


# Function to save links to a file with a timestamp
def save_links_to_file(links):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'news_links_{timestamp}.txt'
    with open(filename, 'w') as file:
        for link in links:
            file.write(f"{link}\n")
    print(f"Links saved to {filename}")


# Number of pages to scrape
max_pages = 7483  # Adjust this number based on the number of pages available or your requirement

# Collect all links
all_links = []

# Start from the last successful page or the beginning
start_page = read_last_page()

# Set up multiprocessing
num_workers = 8  # Number of worker processes

try:
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(scrape_page, page_number) for page_number in range(start_page, max_pages + 1)]

        for future in as_completed(futures):
            page_number, links = future.result()
            if links:
                all_links.extend(links)
            # Save the last successful page number
            save_last_page(page_number)
            # Delay to prevent overwhelming the server
            time.sleep(0.1)
except KeyboardInterrupt:
    print("Keyboard interrupt received. Saving collected links...")
    save_links_to_file(all_links)
except Exception as e:
    print(f"An error occurred: {e}")
    save_links_to_file(all_links)

# Remove duplicates
all_links = list(set(all_links))

# Save links to a file with a timestamp
save_links_to_file(all_links)

print(f"Total links collected: {len(all_links)}")
