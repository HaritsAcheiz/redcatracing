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
import math

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class RedCatScraper:
	base_url: str = 'https://www.redcatracing.com/'
	user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'

	def get_price(self, wholesaleprice):
		float_wholesaleprice = float(wholesaleprice)
		if (wholesaleprice is None) or (float_wholesaleprice == 0) or (wholesaleprice == '0.00'):
			result = "0.00"
		else:
			result = float_wholesaleprice - round(float_wholesaleprice * 5 / 100, 2)

		return f"{result:.2f}"

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
			response = await aclient.get(url, follow_redirects=True)
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
			tree = HTMLParser(data[1])
			script_tags = tree.css_first('script#product-json')
			product_data = json.loads(script_tags.text(strip=True))

			with open('shopify_schema.json', 'r') as file:
				current_product = json.load(file)

			current_product['Handle'] = product_data['id']
			current_product['Title'] = product_data['title']

			product_elem = tree.css_first('div#shopify-section-product')

			desc_elem = tree.css_first('div.content-container')
			desc_overview = self.clean_html(desc_elem.css_first('div.tabs-content-container').html)
			if desc_elem is not None:
				current_product['Body (HTML)'] = desc_overview

			current_product['Vendor'] = 'RCR'

			breadcrumbs = product_elem.css_first('div.container').text(strip=True).split('/')
			current_product['Product Category'] = ' > '.join(breadcrumbs[1:-1])

			current_product['Type'] = product_data['type']
			current_product['Tags'] = ', '.join(product_data['tags'])

			option_labels = product_elem.css('label.product-options-label')
			for index, option_label in enumerate(option_labels, 1):
				current_product[f'Option{index} Name'] = option_label.text(strip=True).split(':')[0]

			option1_values = list()
			option2_values = list()
			option3_values = list()
			variant_skus = list()
			variant_weight = list()
			variant_qty = list()
			variant_cost = list()
			variant_image = list()

			for variant in product_data['variants']:
				if current_product['Option1 Name'] != '':
					if variant['option1'] != 'None':
						option1_values.append(variant['option1'])
				else:
					option1_values = ''

				if current_product['Option2 Name'] != '':
					if variant['option2'] != 'None':
						option2_values.append(variant['option2'])
				else:
					option2_values = ''

				if current_product['Option3 Name'] != '':
					if variant['option3'] != 'None':
						option3_values.append(variant['option3'])
				else:
					option3_values = ''

				variant_skus.append(variant['sku'])
				variant_weight.append(round(variant['weight'] / 100, 2))
				variant_qty.append(10 if variant['available'] else 0)
				variant_cost.append(round(variant['price'] / 100, 2))
				variant_image.append(variant['featured_image']['src'][2:])


			current_product['Option1 Value'] = option1_values
			current_product['Option2 Value'] = option2_values
			current_product['Option3 Value'] = option3_values
			current_product['Variant SKU'] = variant_skus
			current_product['Variant Grams'] = variant_weight
			current_product['Variant Inventory Qty'] = variant_qty
			current_product['Google Shopping / Custom Label 0'] = 'RCR'
			current_product['Variant Image'] = variant_image
			current_product['Cost per item'] = variant_cost
			current_product['Variant Price'] = [self.get_price(x) for x in variant_cost]
			current_product['Variant Compare At Price'] = ''

			product_datas.append(current_product)

		logger.info(product_datas[-1])

		logger.info('Data Extracted!')

	def run(self, urls):
		# products_html = asyncio.run(self.fetch_all(urls))
		# self.insert_to_db(products_html, database_name='redcat.db', table_name='products_src')
		self.get_data()