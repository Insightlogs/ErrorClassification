import boto3
import json
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

class ErrorClassifier:
    def __init__(self, opensearch_host, region):
        self.bedrock_client = boto3.client("bedrock-runtime", region_name=region)
        # self.opensearch_client = OpenSearch(hosts=[{"host": opensearch_host, "port": 443}],  http_auth=auth, use_ssl=True,verify_certs=True ,connection_class=RequestsHttpConnection,pool_maxsize=20)
        # region = 'us-west-2'
        service = 'aoss'
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)

        self.opensearch_client = OpenSearch(
            hosts = [{'host': opensearch_host, 'port': 443}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection,
            pool_maxsize = 20
        )
        # # clientop = OpenSearch(
# #     hosts=[{"host": host, "port": 443}],
# #     http_auth=auth,
# #     use_ssl=True,
# #     verify_certs=True,
# #     connection_class=RequestsHttpConnection,
# #     pool_maxsize=20,
# # )
        
        self.claude_model = "eu.anthropic.claude-3-7-sonnet-20250219-v1:0"
        self.inference_profile_arn = "arn:aws:bedrock:eu-north-1:209479310892:inference-profile/eu.anthropic.claude-3-7-sonnet-20250219-v1:0"

    def search_opensearch(self, embedding):
        body = {
            "size": 1,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": embedding,
                        "k": 1
                    }
                }
            }
        }
        response = self.opensearch_client.search(index="error-classification", body=body)
        # print(response)
        top_hit = response["hits"]["hits"][0]
        return top_hit["_score"], top_hit["_source"]

    def classify_with_claude(self, event):
        prompt = f"Classify the following log and give a category with confidence level:\n\n{event['message']} (Status {event['status']})"
        payload = {
            "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
            "max_tokens_to_sample": 200,
            "temperature": 0.2,
            "top_k": 250,
            "top_p": 1.0,
            "stop_sequences": ["\n"]
        }
        response = self.bedrock_client.invoke_model(
            modelId=self.claude_model,
            body=json.dumps(payload),
            contentType="application/json",
            accept="application/json",
            # inferenceConfig={
            #     "inferenceProfileArn": self.inference_profile_arn
            # }
        )
        result = json.loads(response["body"].read())
        content = result.get("completion", "unknown, 0.5")
        parts = content.split(',')
        label = parts[0].strip()
        confidence = float(parts[1].strip()) if len(parts) > 1 else 0.5
        return label, confidence

    def store_classification(self, document):
        self.opensearch_client.index(index="error-classification", body=document)

    def process_log(self, event, embedding):
        score, nearest = self.search_opensearch(embedding)
        print(score)

        if score < 0.70:
            # label, confidence = self.classify_with_claude(event)
            result = {
                "datetime": event.get("timestamp"),
                "message": event["message"],
                "filePath": event.get("timestamp"),
                # "status": event["status"],
                "embedding": embedding,
                # "label": label,
                # "confidence": confidence,
                # "source": event.get("source"),
                "category": 'Network',
                # "origin": "claude"
            }
            self.store_classification(result)
            # print(f"Classified by Claude → {label} ({confidence})")
            print('ggg')
            return result
        else:
            print(f"Similar log found with score {score}. Category from OpenSearch: {nearest.get('category')}")
            return {
                "label": nearest.get("category", "uncategorized"),
                "confidence": score,
                "origin": "opensearch",
                "Category":nearest.get('category')
            }
