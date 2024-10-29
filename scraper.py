from httpx import AsyncClient
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import os
import asyncio
import duckdb
import json
import logging
import re
from html import escape

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class RedCatScraper:
	base_url: str = 'https://www.redcatracing.com/'
	user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'

	def clean_html(self, html_content):
		# 1. Remove non-standard attributes that Shopify may not recognize
		cleaned_html = re.sub(r'\sdata-[\w-]+="[^"]*"', '', html_content)

		# 2. Encode special characters like apostrophes, quotes, etc.
		cleaned_html = escape(cleaned_html)

		# 3. Decode standard HTML entities back to their original form (e.g., <, >)
		cleaned_html = cleaned_html.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')

		# 4. Remove excessive whitespace between tags
		cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)

		# 5. Ensure spaces between inline elements where necessary
		cleaned_html = re.sub(r'(<span[^>]*>)\s*(<)', r'\1 <', cleaned_html)

		# 6. Remove excess spaces and newlines in text nodes
		cleaned_html = re.sub(r'\s*\n\s*', '', cleaned_html)

		return cleaned_html

	async def fetch(self, aclient, url, limit):
		logger.info(f'Fetching {url}...')
		async with limit:
			response = await aclient.get(url)
			if limit.locked():
				await asyncio.sleep(1)
				response.raise_for_status()
		logger.info(f'Fetching {url}...Completed!')

		return url, response.text

	async def fetch_all(self, urls):
		tasks = []
		headers = {
			'user-agent': self.user_agent
		}
		limit = asyncio.Semaphore(4)
		async with AsyncClient(headers=headers, timeout=120) as aclient:
			for url in urls:
				task = asyncio.create_task(self.fetch(aclient, url=url, limit=limit))
				tasks.append(task)
			htmls = await asyncio.gather(*tasks)

		return htmls

	def insert_to_db(self, htmls, database_name, table_name):
		logger.info('Inserting data to database...')
		if os.path.exists(database_name):
			os.remove(database_name)

		conn = duckdb.connect(database_name)
		curr = conn.cursor()

		try:
			curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT, html BLOB)")

			htmls = [(url, bytes(html, 'utf-8') if not isinstance(html, bytes) else html) for url, html in htmls]
			curr.executemany(f"INSERT INTO {table_name} (url, html) VALUES (?, ?)", htmls)
			conn.commit()

		finally:
			curr.close()
			conn.close()
			logger.info('Data inserted!')

	def get_data(self):
		logger.info('Getting data from database...')
		conn = duckdb.connect("redcat.db")
		curr = conn.cursor()
		curr.execute("SELECT url, html FROM  products_src")
		datas = curr.fetchall()
		product_datas = list()
		for data in datas:
			with open('shopify_schema.json', 'r') as file:
				current_product = json.load(file)
			tree = HTMLParser(data[1])
			# logger.info(data[1])
			current_product['Handle'] = data[0].split('/')[-1].split('?')[0]
			product_elem = tree.css_first('div#shopify-section-product')

			title_elem = product_elem.css_first('h1')
			if title_elem is not None:
				current_product['Title'] = title_elem.text(strip=True)

			desc_elem = tree.css_first('div.content-container')
			desc_overview = self.clean_html(desc_elem.css_first('div.tabs-content-container').html)
			if desc_elem is not None:
				current_product['Body (HTML)'] = desc_overview

			current_product['Vendor'] = 'Redcat'
			breadcrumbs = product_elem.css_first('div.container').text(strip=True)
			current_product['Product Category'] = breadcrumbs.replace('/', ' > ')

			current_product['Type'] = ''

			product_datas.append(current_product)

		logger.info(product_datas[-1])

		logger.info('Data Extracted!')

	def run(self, urls):
		# products_html = asyncio.run(self.fetch_all(urls))
		# self.insert_to_db(products_html, database_name='redcat.db', table_name='products_src')
		self.get_data()