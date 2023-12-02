def get_m3u8(url_path):
    import cfscrape

    scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance
    # Or: scraper = cfscrape.CloudflareScraper()  # CloudflareScraper inherits from requests.Session
    cont = scraper.get(url_path).content
    cont = cont.decode("utf-8")
    test_str = str(cont)
    import re

    regex = r"<title>.*<\/title>"
    matches = re.finditer(regex, test_str, re.MULTILINE)
    title = matches.__next__().group()
    title = title[7:-8]

    regex = r"https.*.m3u8.*?\""
    matches = re.finditer(regex, test_str, re.MULTILINE)
    targetUrl = matches.__next__().group()
    return targetUrl[:-1], title
