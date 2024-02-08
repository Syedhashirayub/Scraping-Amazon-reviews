#for getting Product Tag, Review Title, Review Rating, Review Text
import os 
import requests
from bs4 import BeautifulSoup
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import time
from fake_useragent import UserAgent
import re
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
from swiftshadow.classes import Proxy

processed_urls = set()  # Set to store processed product URLs

# Set up proxy rotation
num_proxy = 12
swift = Proxy(countries=['IN', 'PK', 'BD', 'MY', 'TH', 'KR', 'AE', 'DE'], protocol='https', autoRotate=True, maxProxies=num_proxy, cacheFolder='/Volumes/Hardisc/Unsweet_data/data-managment-main/cachefolder')

# Initialize driver function with proxy support
def initialize_driver():
    option = Options()
    option.add_argument("--headless")
    option.add_argument("--disable-gpu")
    option.add_argument("--no-sandbox")

    # Add User-Agent functionality
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    option.add_argument(f'user-agent={random_user_agent}')
    option.add_argument(f'--proxy-server={swift.proxy()["https"]}')
    driver = webdriver.Chrome(options=option)
    return driver

# Function to normalize Amazon product URLs based on their ASIN
#def normalize_amazon_url(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    asin = query_params.get('asin')
    if asin:
        normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?asin={asin[0]}"
        return normalized_url 
    return url

def get_all_reviews_for_tag(driver, tag_url):
    driver.get(tag_url)
    try:
        wait = WebDriverWait(driver, 25)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-hook='review']")))
    except TimeoutException:
        print(f"No reviews found for tag URL: {tag_url}")
        return []

    reviews = []
    while True:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        review_divs = soup.find_all("div", {"data-hook": "review"})

        for review_div in review_divs:
            review_text = ' '.join(element.strip() for element in review_div.find("span", {"data-hook": "review-body"}).stripped_strings if element)
            
            # Extracting review title
            review_title_element = review_div.find("a", {"data-hook": "review-title"})
            if review_title_element:
                # Extract text including rating text
                full_title_text = review_title_element.get_text(strip=True)
                # Extract rating text
                rating_element = review_title_element.find("i", {"data-hook": "review-star-rating"})
                rating_text = rating_element.get_text(strip=True) if rating_element else ""
                # Remove rating text from full title text
                review_title = full_title_text.replace(rating_text, '').strip()

            # Extracting review rating
            review_rating = rating_text.split()[0] if rating_element else ""

            reviews.append({'text': review_text, 'title': review_title, 'rating': review_rating})

        next_button = soup.find("li", {"class": "a-last"})
        if next_button and next_button.find("a"):
            next_page_url = "https://www.amazon.in" + next_button.find("a")["href"]
            driver.get(next_page_url)
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-hook='review']")))
            except TimeoutException:
                break
            time.sleep(2)
        else:
            break
    return reviews

# Function to get product details
def get_product_details(driver, product_url):
    driver.get(product_url)
    wait = WebDriverWait(driver, 20)
    try:
        wait.until(EC.presence_of_element_located((By.ID, "productTitle")))
        # Scroll to the "customerReviews" section
        customer_reviews_section = wait.until(EC.presence_of_element_located((By.ID, "customerReviews")))
        driver.execute_script("arguments[0].scrollIntoView(true);", customer_reviews_section)
    except TimeoutException:
        # If product title not found, consider product as not available
        return None
    #driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
    #time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    product_name = soup.find("span", {"id": "productTitle"}).get_text(strip=True) if soup.find("span", {"id": "productTitle"}) else "Product name not found"
    product_tags = []
    product_tag_links = []
    all_reviews_url = ""
    lighthut_div = soup.find("div", {"id": "cr-dp-lighthut"})
    if lighthut_div:
        tags = lighthut_div.find_all("span", {"class": "cr-lighthouse-term"})
        for tag in tags:
            tag_a = tag.find_parent('a')
            if tag_a and 'href' in tag_a.attrs:
                tag_url = "https://www.amazon.in" + tag_a['href']
                tag_text = tag.get_text(strip=True)
                product_tags.append(tag_text)
                product_tag_links.append(tag_url)
    else:
        see_all_reviews_div = soup.find("div", {"id": "reviews-medley-footer"})
        if see_all_reviews_div:
            see_all_reviews_link = see_all_reviews_div.find("a", {"data-hook": "see-all-reviews-link-foot"})
            if see_all_reviews_link and 'href' in see_all_reviews_link.attrs:
                all_reviews_url = "https://www.amazon.in" + see_all_reviews_link['href']
            

    return {
        'product_name': product_name,
        'product_url': product_url,
        'product_tags': product_tags,
        'product_tag_links': product_tag_links,
        'all_reviews_url': all_reviews_url
    }

# Process each product URL
def process_product_url(product_id, product_url):
    driver = initialize_driver()
    try:
       
        # Check if URL is already processed
        if product_url in processed_urls:
            print(f"Skipping already processed URL: {product_url}")
            return

        processed_urls.add(product_url)  # Add URL to processed set

        product_details = get_product_details(driver, product_url)
        if product_details is None:
            # Product not available, add to not processed file
            not_processed_writer.writerow([product_id, product_url])
        elif product_details['product_tags']:
            for tag, tag_url in zip(product_details['product_tags'], product_details['product_tag_links']):
                reviews = get_all_reviews_for_tag(driver, tag_url)
                for review in reviews:
                    csv_writer.writerow([product_id, product_details['product_name'], product_details['product_url'], tag, review['title'], review['rating'], review['text']])
        else:
            all_reviews_url = product_details.get('all_reviews_url', '')
            if not all_reviews_url:
                not_processed_writer.writerow([product_id, product_url])
            else:
                reviews = get_all_reviews_for_tag(driver, all_reviews_url)
                for review in reviews:
                    csv_writer.writerow([product_id, product_details['product_name'], product_details['product_url'], "", review['title'], review['rating'], review['text']])
    
    finally:
        driver.quit()

# Main execution
if __name__ == "__main__":
    input_csv_filename = "/Volumes/Hardisc/Unsweet_data/data-managment-main/makeup_product_links28583 copy.csv"
    output_csv_filename = "/Volumes/Hardisc/Unsweet_data/data-managment-main/makeup_tagreviews_data_6000.csv"

    not_processed_csv_filename = "/Volumes/Hardisc/Unsweet_data/data-managment-main/Products_not_processed.csv"

    with open(input_csv_filename, mode='r', newline='', encoding='utf-8') as infile:
        csv_reader = csv.reader(infile)
        products = [(row[0], row[1]) for row in csv_reader]

    with open(output_csv_filename, mode='w', newline='', encoding='utf-8') as outfile, \
         open(not_processed_csv_filename, mode='w', newline='', encoding='utf-8') as not_processed_file:
        csv_writer = csv.writer(outfile)
        not_processed_writer = csv.writer(not_processed_file)
        csv_writer.writerow(["Product ID", "Product name", "Product URL", "Product Tag", "Review Title", "Review Rating", "Review Text"])
        not_processed_writer.writerow(["Product ID", "Product URL"])

        with ThreadPoolExecutor(max_workers=num_proxy) as executor:
            executor.map(lambda x: process_product_url(*x), products)