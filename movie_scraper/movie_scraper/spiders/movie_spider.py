import scrapy
import re
from movie_scraper.middlewares import RotateUserAgentMiddleware, RotateProxiesMiddleware


class MovieSpider(scrapy.Spider):
    name = "movie_spider"
    allowed_domains = ["ru.wikipedia.org", "imdb.com"]
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]

    film_count = 0
    max_films = 1

    custom_settings = {
        "RETRY_TIMES": 5,
        "RETRY_HTTP_CODES": [503],
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "movie_scraper.middlewares.RotateUserAgentMiddleware": 543,
            "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 1,
            "movie_scraper.middlewares.RotateProxiesMiddleware": 543,
        },
        "DOWNLOAD_TIMEOUT": 15,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_IP": 1,
        # CSV export settings
        "FEEDS": {
            "movies.csv": {
                "format": "csv",
                "fields": [
                    "Название",
                    "Жанр",
                    "Режиссёр",
                    "Страна",
                    "Год",
                    "IMDB Rating",
                ],
                "overwrite": True,  # Overwrite the file each time
            }
        },
    }

    def start_requests(self):
        if self.film_count >= self.max_films:
            return
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse_category)

    def parse_category(self, response):
        if self.film_count >= self.max_films:
            return

        movie_links = response.css(
            "#mw-pages div.mw-category-group a::attr(href)"
        ).getall()
        for link in movie_links:
            if self.film_count >= self.max_films:
                break
            yield response.follow(link, callback=self.parse_moviepage)

        next_page_link = response.css(
            'a:contains("Следующая страница")::attr(href)'
        ).get()
        if next_page_link and self.film_count < self.max_films:
            yield response.follow(
                response.urljoin(next_page_link), callback=self.parse_category
            )

    def parse_moviepage(self, response):
        if self.film_count >= self.max_films:
            self.logger.info(f"Max films reached: {self.film_count}")
            return

        def clean_and_join(selector):
            cleaned = ", ".join(
                re.sub(r"\[\d+\]|\[…\]|\n|\xa0|\[d\]", "", text)
                for text in selector.getall()
            )
            cleaned = re.sub(
                r"[/\(\)—]|рус.|англ.|\[en\]|ru|en|, ,", "", cleaned
            ).strip(", ")
            return cleaned

        title = (
            response.css("table.infobox th.infobox-above::text").get(default="").strip()
        )
        genre = clean_and_join(
            response.css('table.infobox tr th:contains("Жанр") + td ::text')
        )
        director = clean_and_join(
            response.css('table.infobox tr th:contains("Режисс") + td ::text')
        )
        country = clean_and_join(
            response.css('table.infobox tr th:contains("Стран") + td ::text')
        )

        year = ", ".join(
            re.findall(
                r"\d{4}",
                ", ".join(
                    response.css(
                        'table.infobox tr th:contains("Год") + td ::text'
                    ).getall()
                ),
            )
        )

        imdb_link = response.css('a[href*="imdb.com"]::attr(href)').get()

        if imdb_link:
            yield response.follow(
                imdb_link,
                callback=self.parse_imdb,
                meta={
                    "title": title,
                    "genre": genre,
                    "director": director,
                    "country": country,
                    "year": year,
                },
            )
        else:
            yield self.create_movie_item(
                title, genre, director, country, year, "Не указан"
            )
            self.film_count += 1

    def parse_imdb(self, response):
        if self.film_count >= self.max_films:
            return

        if "IMDb" not in response.text:
            self.logger.error(f"IMDb blocking detected at {response.url}")
            return

        rating_selector = 'div[data-testid="hero-rating-bar__aggregate-rating__score"] span:first-child::text'
        imdb_rating = response.css(rating_selector).get()

        if not imdb_rating:
            imdb_rating = response.css(".sc-bde20123-1.iZLXmJ::text").get()

        imdb_rating = imdb_rating.strip() if imdb_rating else "Не указан"

        yield self.create_movie_item(
            response.meta["title"],
            response.meta["genre"],
            response.meta["director"],
            response.meta["country"],
            response.meta["year"],
            imdb_rating,
        )
        self.film_count += 1

    def create_movie_item(self, title, genre, director, country, year, imdb_rating):
        return {
            "Название": title,
            "Жанр": genre,
            "Режиссёр": director,
            "Страна": country,
            "Год": year,
            "IMDB Rating": imdb_rating,
        }
