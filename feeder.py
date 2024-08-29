import feedparser
import requests
from bs4 import BeautifulSoup

rss_urls = [
    'https://iclnoticias.com.br/feed/',
    'https://www.infomoney.com.br/feed/'
]

def fetch_feeds(urls):
    feeds = []
    for url in urls:
        feed = feedparser.parse(url)
        feeds.append(feed)
    return feeds

def fetch_full_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.find_all('p')
        full_content = ' '.join([para.get_text() for para in paragraphs])
        return full_content
    except requests.RequestException as e:
        print(f"Error fetching content from {url}: {e}")
        return ''

def filter_entries(entries, keyword):
    filtered = []
    for entry in entries:
        full_content = fetch_full_content(entry.link)
        if keyword.lower() in entry.title.lower() or keyword.lower() in entry.summary.lower() or keyword.lower() in full_content.lower():
            filtered.append(entry)
    return filtered

def save_filtered_entries_to_file(feeds, keyword, filename):
    with open(filename, 'w', encoding='utf-8') as file:  # Especifica a codificação UTF-8
        for feed in feeds:
            filtered_entries = filter_entries(feed.entries, keyword)
            file.write(f"Feed Title: {feed.feed.title}\n")
            for entry in filtered_entries:
                file.write(f"Title: {entry.title}\n")
                file.write(f"Link: {entry.link}\n")
                file.write(f"Published: {entry.published}\n")
                file.write(f"Summary: {entry.summary}\n")
                full_content = fetch_full_content(entry.link)
                file.write(f"Full Content: {full_content}\n\n")

def main():
    feeds = fetch_feeds(rss_urls)
    keyword = 'Vale'
    save_filtered_entries_to_file(feeds, keyword, 'filtered_news.txt')

if __name__ == "__main__":
    main()
