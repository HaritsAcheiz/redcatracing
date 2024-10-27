from scraper import RedCatScraper

if __name__ == '__main__':
	urls = ['https://www.redcatracing.com/collections/featured-products/products/redcat-valkyrie-tr-rc-offroad-truggy-1-10-4s-brushless-electric-truggy?variant=41323077173338',
			'https://www.redcatracing.com/collections/featured-products/products/redcat-valkyrie-mt-rc-offroad-truck-1-10-4s-brushless-electric-truck?variant=41323035459674'
	]

	scraper = RedCatScraper()
	scraper.run(urls)
