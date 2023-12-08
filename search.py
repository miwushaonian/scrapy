from qdrant_client.http.models import Distance, VectorParams
from qdrant_client import QdrantClient
from towhee import ops
import cv2
from qdrant_client.http.models import PointStruct
from PIL import Image
import numpy as np
import base64
import os

img_encoder = ops.image_embedding.timm(model_name="resnet50")
client = QdrantClient("wu.bybyte.cn", port=3333)
img = cv2.imread("./ttttt.png")
feature = img_encoder(img)
hists = client.search(collection_name="video", query_vector=feature.tolist(), limit=10)
os.makedirs("results", exist_ok=True)
for index, hist in enumerate(hists):
    tmp = base64.b64decode(hist.payload["thumb"])
    title_b64 = base64.b64decode(hist.payload["title"])
    title = title_b64.decode()
    thumb = np.frombuffer(tmp, dtype=np.uint8)
    thumb = thumb.view(np.uint8).reshape((128, 128, 3))
    cv2.imwrite(f"./results/{index}_{hist.score}_{title}.png", thumb)
