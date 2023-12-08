from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

client = QdrantClient(host="127.0.0.1", port=7333)
search_result = client.search(
    collection_name="count",
    query_vector=[0],
    query_filter=Filter(
        must=[FieldCondition(key="id", match=MatchValue(value="912843"))]
    ),
    with_payload=True,
    limit=1,
)

print(search_result[0].id)
