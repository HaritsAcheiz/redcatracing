from httpx import AsyncClient
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import os
import asyncio
import duckdb
import json


@dataclass
class RedCatScraper:
	base_url: str = 'https://www.redcatracing.com/'
	user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'

	async def fetch(self, aclient, url, limit):
		print(f'Fetching {url}...')
		async with limit:
			response = await aclient.get(url)
			if limit.locked():
				await asyncio.sleep(1)
				response.raise_for_status()
		print(f'Fetching {url}...Completed!')

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
		if os.path.exists(database_name):
			os.remove(database_name)
		conn = duckdb.connect(database_name)
		curr = conn.cursor()
		curr.execute(
			f"""
			CREATE TABLE IF NOT EXISTS {table_name}(
			url TEXT,
			html BLOB
			)
			"""
		)

		for url, html in htmls:
			html_blob = bytes(html, 'utf-8')
			curr.execute(
				f"INSERT INTO {table_name} (url, html) VALUES(?,?)",
				(url, html_blob)
			)
			conn.commit()

	def get_data(self):
		conn = duckdb.connect("redcat.db")
		curr = conn.cursor()
		curr.execute("SELECT url, html FROM  products_src")
		datas = curr.fetchall()
		product_datas = list()
		for data in datas:
			with open('shopify_schema.json', 'r') as file:
				current_product = json.load(file)
			tree = HTMLParser(data[1])
			product_elem = tree.css_first('div.card.card--collapsed.card--sticky')
			current_product['Handle'] = data[0].split('/')[-1]
			print(current_product)

	def run(self, urls):
		products_html = asyncio.run(self.fetch_all(urls))
		self.insert_to_db(products_html, database_name='redcat.db', table_name='products_src')
		self.get_data()