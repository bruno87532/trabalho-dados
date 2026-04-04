from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper.http_client import HttpClient
from scraper.parser import BookParser
from common.functions import format_author, clean_text_tag

class GutenbergScraper:
    def __init__(self):
        self.url = "https://www.gutenberg.org"
        self.client = HttpClient()
        self.parser = BookParser()
        self.author_cache = {}

    def extract(self, items=1100):
        results = []
        start_index = 1
        collected = 0

        while collected < items:
            url = f"{self.url}/ebooks/search/?sort_order=downloads&start_index={start_index}"
            response = self.client.get(url)

            if not response:
                start_index += 25
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            books = soup.select("li.booklink")

            if not books:
                break

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(self._process_book, book) for book in books]

                for future in as_completed(futures):
                    result = future.result()

                    if result:
                        results.append(result)
                        collected += 1

                        if collected >= items:
                            break

            start_index += 25

        return results

    def _process_book(self, book):
        try:
            base_data = self.parser.parse_list_item(book)
            response = self.client.get(f"{self.url}{base_data['link']}")
            if not response:
                return None

            soup = BeautifulSoup(response.text, "html.parser")
            details = self.parser.parse_book_page(soup)
            authors = self._handle_author(soup)

            base_data.update(details)
            base_data["authors"] = authors
            return base_data
        except:
            return None

    def _handle_author(self, soup):
        authors = soup.select('a[itemprop="creator"]')
        result = {}

        for author in authors:
            name = format_author(clean_text_tag(author))

            if name in self.author_cache:
                result[name] = self.author_cache[name]
                continue

            start_index = 1
            total = 0
            author_link = author["href"]

            while True:
                response = self.client.get(f"{self.url}{author_link}?start_index={start_index}")
                if not response:
                    break

                soup = BeautifulSoup(response.text, "html.parser")
                books = soup.select("li.booklink")

                if not books:
                    break

                total += len(books)
                start_index += 25

            self.author_cache[name] = total
            result[name] = total

        return result