import cfscrape
import time
import multiprocessing
import os
import sys
import argparse
import base64
from towhee import ops
import cv2
import tqdm
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from qdrant_client.http.models import PointStruct
import pickle
import hashlib
from qdrant_client.http.models import Filter, FieldCondition, MatchValue


def myhash(s):
    z = hashlib.md5(s.encode()).hexdigest()
    r = ""
    # a to z
    for i in range(26):
        r = chr(ord("a") + i)
        z = z.replace(r, str(i))
    return int(z[:12])


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


import os
import re
import sys
import queue
import base64
import platform
import requests
import urllib3
import subprocess
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import shutil


class ThreadPoolExecutorWithQueueSizeLimit(ThreadPoolExecutor):
    """
    实现多线程有界队列
    队列数为线程数的2倍
    """

    def __init__(self, max_workers=None, *args, **kwargs):
        super().__init__(max_workers, *args, **kwargs)
        self._work_queue = queue.Queue(max_workers * 2)


def make_sum():
    ts_num = 0
    while True:
        yield ts_num
        ts_num += 1


class M3u8Download:
    """
    :param url: 完整的m3u8文件链接 如"https://www.bilibili.com/example/index.m3u8"
    :param name: 保存m3u8的文件名 如"index"
    :param max_workers: 多线程最大线程数
    :param num_retries: 重试次数
    :param base64_key: base64编码的字符串
    """

    def __init__(self, url, name, max_workers=64, num_retries=5, base64_key=None):
        self._url = url
        self._name = name
        self._max_workers = max_workers
        self._num_retries = num_retries
        self._file_path = os.path.join(os.getcwd(), self._name)
        self._front_url = None
        self._ts_url_list = []
        self._success_sum = 0
        self._ts_sum = 0
        self._key = base64.b64decode(base64_key.encode()) if base64_key else None
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36"
        }

        urllib3.disable_warnings()

        self.get_m3u8_info(self._url, self._num_retries)
        with ThreadPoolExecutorWithQueueSizeLimit(self._max_workers) as pool:
            for k, ts_url in enumerate(self._ts_url_list):
                pool.submit(
                    self.download_ts,
                    ts_url,
                    os.path.join(self._file_path, str(k)),
                    self._num_retries,
                )
        if self._success_sum == self._ts_sum:
            self.output_mp4()
            self.delete_file()

    def get_m3u8_info(self, m3u8_url, num_retries):
        """
        获取m3u8信息
        """
        try:
            with requests.get(
                    m3u8_url, timeout=(3, 30), verify=False, headers=self._headers
            ) as res:
                self._front_url = res.url.split(res.request.path_url)[0]
                if "EXT-X-STREAM-INF" in res.text:  # 判定为顶级M3U8文件
                    for line in res.text.split("\n"):
                        if "#" in line:
                            continue
                        elif line.startswith("http"):
                            self._url = line
                        elif line.startswith("/"):
                            self._url = self._front_url + line
                        else:
                            self._url = self._url.rsplit("/", 1)[0] + "/" + line
                    self.get_m3u8_info(self._url, self._num_retries)
                else:
                    m3u8_text_str = res.text
                    self.get_ts_url(m3u8_text_str)
        except Exception as e:
            print(e)
            if num_retries > 0:
                self.get_m3u8_info(m3u8_url, num_retries - 1)

    def get_ts_url(self, m3u8_text_str):
        """
        获取每一个ts文件的链接
        """
        if not os.path.exists(self._file_path):
            os.mkdir(self._file_path)
        new_m3u8_str = ""
        ts = make_sum()
        for line in m3u8_text_str.split("\n"):
            if "#" in line:
                if "EXT-X-KEY" in line and "URI=" in line:
                    if os.path.exists(os.path.join(self._file_path, "key")):
                        continue
                    key = self.download_key(line, 5)
                    if key:
                        new_m3u8_str += f"{key}\n"
                        continue
                new_m3u8_str += f"{line}\n"
                if "EXT-X-ENDLIST" in line:
                    break
            else:
                if line.startswith("http"):
                    self._ts_url_list.append(line)
                elif line.startswith("/"):
                    self._ts_url_list.append(self._front_url + line)
                else:
                    self._ts_url_list.append(self._url.rsplit("/", 1)[0] + "/" + line)
                new_m3u8_str += os.path.join(self._file_path, str(next(ts))) + "\n"
        self._ts_sum = next(ts)
        with open(self._file_path + ".m3u8", "wb") as f:
            if platform.system() == "Windows":
                f.write(new_m3u8_str.encode("gbk"))
            else:
                f.write(new_m3u8_str.encode("utf-8"))

    def download_ts(self, ts_url, name, num_retries):
        """
        下载 .ts 文件
        """
        ts_url = ts_url.split("\n")[0]
        try:
            if not os.path.exists(name):
                with requests.get(
                        ts_url,
                        stream=True,
                        timeout=(5, 60),
                        verify=False,
                        headers=self._headers,
                ) as res:
                    if res.status_code == 200:
                        with open(name, "wb") as ts:
                            for chunk in res.iter_content(chunk_size=1024):
                                if chunk:
                                    ts.write(chunk)
                        self._success_sum += 1
                        # sys.stdout.write(
                        #     "\r[%-25s](%d/%d)"
                        #     % (
                        #         "*" * (100 * self._success_sum // self._ts_sum // 4),
                        #         self._success_sum,
                        #         self._ts_sum,
                        #     )
                        # )
                        sys.stdout.flush()
                    else:
                        self.download_ts(ts_url, name, num_retries - 1)
            else:
                self._success_sum += 1
        except Exception:
            if os.path.exists(name):
                os.remove(name)
            if num_retries > 0:
                self.download_ts(ts_url, name, num_retries - 1)

    def download_key(self, key_line, num_retries):
        """
        下载key文件
        """
        mid_part = re.search(r"URI=[\'|\"].*?[\'|\"]", key_line).group()
        may_key_url = mid_part[5:-1]
        if self._key:
            with open(os.path.join(self._file_path, "key"), "wb") as f:
                f.write(self._key)
            return f'{key_line.split(mid_part)[0]}URI="./{self._name}/key"'
        if may_key_url.startswith("http"):
            true_key_url = may_key_url
        elif may_key_url.startswith("/"):
            true_key_url = self._front_url + may_key_url
        else:
            true_key_url = self._url.rsplit("/", 1)[0] + "/" + may_key_url
        try:
            with requests.get(
                    true_key_url, timeout=(5, 30), verify=False, headers=self._headers
            ) as res:
                with open(os.path.join(self._file_path, "key"), "wb") as f:
                    f.write(res.content)
            return f'{key_line.split(mid_part)[0]}URI="./{self._name}/key"{key_line.split(mid_part)[-1]}'
        except Exception as e:
            print(e)
            if os.path.exists(os.path.join(self._file_path, "key")):
                os.remove(os.path.join(self._file_path, "key"))
            print("加密视频,无法加载key,揭秘失败")
            if num_retries > 0:
                self.download_key(key_line, num_retries - 1)

    """
    run cmd
    """

    def shell_run_cmd_block(self, cmd):
        p = subprocess.Popen(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        p.wait()
        # print("cmd ret=%d" % p.returncode)

    def output_mp4(self):
        """
        合并.ts文件，输出mp4格式视频，需要ffmpeg
        """
        cmd = (
                'ffmpeg -loglevel quiet -y -allowed_extensions ALL -i "%s.m3u8" -acodec copy -vcodec copy -f mp4 %s.mp4'
                % (self._file_path, self._name)
        )
        # os.system(cmd)
        self.shell_run_cmd_block(cmd)

    def delete_file(self):
        file = os.listdir(self._file_path)
        for item in file:
            os.remove(os.path.join(self._file_path, item))
        os.removedirs(self._file_path)
        os.remove(self._file_path + ".m3u8")


def proc(url_list, name_list):
    sta = len(url_list) == len(name_list)
    for i, u in enumerate(url_list):
        M3u8Download(
            u,
            name_list[i] if sta else f"{name_list[0]}{i + 1:02}",
            max_workers=64,
            num_retries=10,
            # base64_key='5N12sDHDVcx1Hqnagn4NJg=='
        )


def f(index, k, args):
    vid = int(k[6:-4])
    search_result = client.search(
        collection_name="count",
        query_vector=[0],
        query_filter=Filter(
            must=[FieldCondition(key="id", match=MatchValue(value=str(vid)))]
        ),
        with_payload=True,
        limit=1,
    )
    if len(search_result) > 0 and search_result[0].id == vid:
        print(f"{k} has insert")
        return

    m3u8_url, title = get_m3u8(f"https://hsex.men/{k}")
    title_b64 = base64.b64encode(title.encode())
    proc([m3u8_url], [f"{k}"])
    if os.path.exists(f"{k}.mp4") == False:
        # print(f"{k}.mp4 not exists")
        lg = open("log.txt", "a")
        lg.write(f"{k}.mp4 not exists0\n")
        lg.close()
        return
    import shutil

    shutil.move(f"{k}.mp4", f"{k}.data")
    shell_cmd = f"""ffmpeg  -loglevel quiet -i {k}.data -vf "select=eq(pict_type\,I)" -vsync vfr -qscale:v 2 -f image2pipe - | ffmpeg -loglevel quiet  -f image2pipe -i - -c:v libx264 -r 30 {k}.mp4
                    """
    subprocess.Popen(
        shell_cmd,
        shell=True,
    ).wait()
    os.remove(f"{k}.data")
    if os.path.exists(f"{k}.mp4") == False:
        # print(f"{k}.mp4 not exists")
        lg = open("log.txt", "a")
        lg.write(f"{k}.mp4 not exists\n")
        lg.close()
        return
    cap = cv2.VideoCapture(f"{k}.mp4")
    fpsn = 0
    batch_data = []
    batch_data_count = []
    while True:
        ret, frame = cap.read()
        if ret == False:
            break
        fpsn = fpsn + 1
        if fpsn >= 0:
            vid = int(k[6:-4])
            feature = img_encoder(frame)
            thumb = cv2.resize(frame, (128, 128))
            imgdata_thumb = base64.b64encode(thumb.tobytes()).decode("utf-8")
            finaly_id = vid * 10000 + fpsn
            batch_data.append(
                PointStruct(
                    id=finaly_id,
                    vector=feature.tolist(),
                    payload={
                        "fpsn": str(fpsn),
                        "title": title_b64,
                        "thumb": imgdata_thumb,
                    },
                )
            )
            batch_data_count.append(
                PointStruct(
                    id=vid,
                    vector=[0],
                    payload={
                        "id": str(vid),
                    },
                )
            )
        if len(batch_data) >= 100 and args.tdb:
            client.upsert(collection_name="video", points=batch_data, wait=True)
            batch_data = []
        if len(batch_data_count) >= 100 and args.tdb:
            client.upsert(collection_name="count", points=batch_data_count, wait=True)
            batch_data_count = []

    if len(batch_data) > 0 and args.tdb:
        client.upsert(collection_name="video", points=batch_data, wait=False)
    if len(batch_data_count) > 0 and args.tdb:
        client.upsert(collection_name="count", points=batch_data_count, wait=False)
    if False == args.tdb:
        # 序列化
        output = open(f"{k}.fea", "wb")
        pickle.dump(batch_data, output)
        pass
    cap.release()
    os.remove(f"{k}.mp4")
    print(f"{index}-{k} - {fpsn}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="argparse")
    parser.add_argument(
        "-s", type=str, default="127.0.0.1", help="qdrant server addr"
    )
    parser.add_argument("-p", type=int, default=7333, help="qdrant server port")
    parser.add_argument("-page", type=int, default=1, help="age of the programmer")
    parser.add_argument("-tdb", type=bool, default=True)
    parser.add_argument("-key", type=str, default=None, help="qdrant api key")
    parser.add_argument("-ep", type=int, default=0)
    args = parser.parse_args()
    print(args)
    i = args.page
    scraper = cfscrape.create_scraper()  # returns a CloudflareScraper instance
    # Or: scraper = cfscrape.CloudflareScraper()  # CloudflareScraper inherits from requests.Session
    retrys = 0
    img_encoder = ops.image_embedding.timm(model_name="resnet50")
    if args.tdb:
        client = QdrantClient(args.s, port=args.p, api_key=args.key)
        try:
            client.get_collection("video")
        except:
            client.create_collection(
                collection_name="video",
                vectors_config=VectorParams(size=2048, distance=Distance.COSINE),
            )
        try:
            client.get_collection("count")
        except:
            client.create_collection(
                collection_name="count",
                vectors_config=VectorParams(size=1, distance=Distance.COSINE),
            )
    while (args.ep == 0) or (i <= args.ep):
        try:
            cur_page = open("page", "w")
            if args.tdb:
                client = QdrantClient(args.s, port=args.p, api_key=args.key)
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
            print(f"Now scrapy page {i},total {len(matches_url)}")
            if 0 == len(matches_url):
                # if retrys >= 50:
                #     i = 1
                #     retrys = 0
                #     print("No more page, return to 1")
                # else:
                print(f"No more page, retry X times {retrys}")
                retrys = retrys + 1
                continue
            else:
                retrys = 0
                i = i + 1
            cur_page.write(str(i))
            cur_page.close()
            process = tqdm.tqdm(matches_url)
            for index, k in enumerate(process):
                while True:
                    try:
                        import tempfile

                        temp_folder = tempfile.mkdtemp()
                        cur_dir = os.getcwd()
                        os.chdir(temp_folder)
                        f(index, k, args)
                        os.chdir(cur_dir)
                        break
                    except Exception as e:
                        print(e)
                        pass
            pass
        except Exception as e:
            print(f"抓取页面失败 {e}")
            continue
