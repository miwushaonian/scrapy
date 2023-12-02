from qdrant_client.http.models import Distance, VectorParams
from qdrant_client import QdrantClient
from towhee import ops
import cv2
from qdrant_client.http.models import PointStruct
from PIL import Image

img_encoder = ops.image_embedding.timm(model_name="resnet50")
client = QdrantClient("10.131.7.124", port=6333)
img = cv2.imread("./ttttt.png")
feature = img_encoder(img)
hists = client.search(collection_name="video", query_vector=feature.tolist())
for hist in hists:
    print(hist)
