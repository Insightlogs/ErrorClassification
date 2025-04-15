import boto3
import json
# from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# region = "eu-north-1"

# # Initialize the Bedrock runtime client
# client = boto3.client("bedrock-runtime", region_name=region)  # Change region if needed


# def get_cohere_embedding(text):
#     # payload = {
#     #     "modelId": "amazon.titan-embed-text-v2:0",
#     #     "contentType": "application/json",
#     #     "accept": "/",
#     #     "body": json.dumps(
#     #         {
#     #             "texts": [text],
#     #             "input_type": "search_query",
#     #         }
#     #     ),
#     # }

#     # response = client.invoke_model(**payload)
#     # result = json.loads(response["body"].read().decode("utf-8"))

#     # Format the input combining status code and message
#     # formatted_input = f"Status {status_code}: {log_message}"

#     # Create the payload with the formatted input
#     payload = {
#         'inputText': text
#     }

#         # Invoke the model to generate embeddings
#     response = client.invoke_model(
#         modelId='amazon.titan-embed-text-v2:0',  # Ensure the model ID is correct
#         body=json.dumps(payload),
#         contentType='application/json',
#         accept='application/json'
#     )

#     result = json.loads(response["body"].read().decode("utf-8"))

#     # response_body = json.loads(response.get('body').read())
#     # embedding = response_body.get("embedding")
#     # print(f"The embedding vector has {len(result)} values\n{result[0:3]+['...']+result[-3:]}")
#     # print(json.dumps(result, indent=2))

#     return result["embedding"] # Extract the embedding vector


# # Example text
# # text = "test text"
# # embedding = get_cohere_embedding(text)

# # print("Generated Embedding:", embedding)

# # host = 'akoanqav21wglrnpiu4b.eu-north-1.aoss.amazonaws.com' # cluster endpoint, for example: my-test-domain.us-east-1.aoss.amazonaws.com
# # region = 'eu-north-1'
# # service = 'aoss'
# # credentials = boto3.Session().get_credentials()
# # auth = AWSV4SignerAuth(credentials, region, service)
# # clientop = OpenSearch(
# #     hosts=[{"host": host, "port": 443}],
# #     http_auth=auth,
# #     use_ssl=True,
# #     verify_certs=True,
# #     connection_class=RequestsHttpConnection,
# #     pool_maxsize=20,
# # )

# # # print("Connected to OpenSearch!")
# # # index_name = "items"
# # document = {
# #   'embedding': embedding,
# #   'category': 'test',
# #   'datetime': '2025-04-13T15:30:00Z',
# #   'filePath':'test',
# #   'message':'test'
# # }

# # # # Insert into OpenSearch
# # response = clientop.index(
# #     index = 'error-classification',
# #     body = document,

# # )
# # response = clientos.index(index=index_name, body=doc)
# # print("Document inserted:", response)


class EmbeddingGenerator:
    def __init__(self, region):
        self.client = boto3.client("bedrock-runtime", region_name=region)
        self.model_id = "cohere.embed-english-v3"

    def get_embedding(self, text):
        payload = {
            "texts": [text],
            "input_type": "search_query"
        }

        response = self.client.invoke_model(
            modelId=self.model_id,
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json"
        )

        result = json.loads(response["body"].read().decode("utf-8"))
        print(result)
        return result["embeddings"]