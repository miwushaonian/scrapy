from qdrant_client.http.models import Distance, VectorParams
from qdrant_client import QdrantClient
from towhee import ops
import cv2
from qdrant_client.http.models import PointStruct

img_encoder = ops.image_embedding.timm(model_name="resnet50")
client = QdrantClient("10.131.7.124", port=6333)
try:
    client.get_collection("video")
except:
    client.create_collection(
        collection_name="video",
        vectors_config=VectorParams(size=2048, distance=Distance.COSINE),
    )
cap = cv2.VideoCapture("./a.mp4")
fpsn = 0
while True:
    ret, frame = cap.read()
    if ret == False:
        break
    fpsn = fpsn + 1
    if fpsn % 24 == 0:
        feature = img_encoder(frame)
        client.upsert(
            collection_name="video",
            points=[PointStruct(id=fpsn, vector=feature.tolist())],
        )
        pass
