import cfscrape
import time
import sub
import m3
import multiprocessing
import os
import sys

os.makedirs("video", exist_ok=True)
if len(sys.argv) >= 2:
    start_page = sys.argv[1]
else:
    start_page = 1
if __name__ == "__main__":
    i = start_page
    scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance
    # Or: scraper = cfscrape.CloudflareScraper()  # CloudflareScraper inherits from requests.Session
    retrys = 0
    while True:
        try:
            cont = scraper.get(f"https://hsex.men/list-{i}.htm").content
            cont = cont.decode("utf-8")
            test_str = str(cont)
            import re

            regex = r"video-[0-9]*.htm"
            matches = re.finditer(regex, test_str, re.MULTILINE)
            matches_url = []
            for matchNum, match in enumerate(matches, start=1):
                matches_url.append(match.group())
            matches_url = list(set(matches_url))
            if 0 == len(matches_url):
                if retrys >= 50:
                    i = 1
                    retrys = 0
                    print("No more page, return to 1")
                else:
                    print(f"No more page, retry 5 times {retrys}")
                    retrys = retrys + 1
                continue
            else:
                retrys = 0
                i = i + 1
            print(f"Now scrapy page {i},total {len(matches_url)}")
            for k in matches_url:
                try:
                    m3u8_url, title = sub.get_m3u8(f"https://hsex.men/{k}")
                    m3.proc([m3u8_url], [f"{k}"])
                    import shutil

                    shutil.move(f"{k}.mp4", f"video/{k}.data")
                except Exception as e:
                    print(f"下载失败 {k} {e}")
                    continue
            pass
        except Exception as e:
            print(f"下载失败 {e}")
            continue
