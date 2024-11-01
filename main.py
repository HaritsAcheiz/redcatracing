from scraper import RedCatScraper

if __name__ == '__main__':
	urls = [
		'https://www.redcatracing.com/products/redcat-valkyrie-tr-rc-offroad-truggy-1-10-4s-brushless-electric-truggy?variant=41323077173338',
		'https://www.redcatracing.com/products/redcat-valkyrie-mt-rc-offroad-truck-1-10-4s-brushless-electric-truck?variant=41323035459674',
		'https://www.redcatracing.com/products/sixtyfour?variant=51641499681135'
	]
	search_results_url = 'https://www.redcatracing.com/pages/search-results?findify_offset=3600'

	scraper = RedCatScraper()
	scraper.run(urls)
