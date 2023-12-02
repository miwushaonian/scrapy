import cfscrape
import time
import sub
import m3
import multiprocessing
import os
import sys
import argparse
import base64
from towhee import ops
import cv2
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.models import PointStruct

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="argparse")
    parser.add_argument(
        "-s", type=str, default="10.131.7.124", help="qdrant server addr"
    )
    parser.add_argument("-p", type=int, default=6333, help="qdrant server port")
    parser.add_argument("-page", type=int, default=1, help="age of the programmer")

    args = parser.parse_args()
    print(args)
    i = args.page
    scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance
    # Or: scraper = cfscrape.CloudflareScraper()  # CloudflareScraper inherits from requests.Session
    retrys = 0
    img_encoder = ops.image_embedding.timm(model_name="resnet50")
    client = QdrantClient(args.s, port=args.p)
    try:
        client.get_collection("video")
    except:
        client.create_collection(
            collection_name="video",
            vectors_config=VectorParams(size=2048, distance=Distance.COSINE),
        )
    while True:
        try:
            client = QdrantClient(args.s, port=args.p)
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
                    title_b64 = base64.b64encode(title.encode())
                    m3.proc([m3u8_url], [f"{k}"])
                    import shutil

                    # shutil.move(f"{k}.mp4", f"video/{k}.data")
                    cap = cv2.VideoCapture(f"{k}.mp4")
                    fpsn = 0
                    batch_data = []
                    while True:
                        ret, frame = cap.read()
                        if ret == False:
                            break
                        fpsn = fpsn + 1
                        if fpsn % 24 == 0:
                            vid = int("".join([str(ord(x)) for x in f"{k[6:-4]}"]))
                            feature = img_encoder(frame)
                            batch_data.append(
                                PointStruct(
                                    id=vid * 1000000 + fpsn,
                                    vector=feature.tolist(),
                                    payload={"fpsn": str(fpsn), "title": title_b64},
                                )
                            )
                        if len(batch_data)>=12:
                                client.upsert(collection_name="video",points=batch_data)
                                batch_data=[]
                            
                    if len(batch_data)>0:
                        client.upsert(collection_name="video",points=batch_data)
                    cap.release()
                    os.remove(f"{k}.mp4")

                except Exception as e:
                    print(f"下载失败 {k} {e}")
                    continue
            pass
        except Exception as e:
            print(f"下载失败 {e}")
            continue
