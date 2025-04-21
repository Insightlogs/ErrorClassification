import boto3
import json
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from datetime import datetime


class ErrorClassifier:
    def __init__(self, opensearch_host, region):
        self.bedrock_client = boto3.client("bedrock-runtime", region_name=region)
        service = 'aoss'
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)

        self.opensearch_client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
        self.claude_model_id = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"

    def search_opensearch(self, embedding):
        body = {
            "size": 10,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": embedding,
                        "k": 10
                    }
                }
            }
        }
        # print(type(embedding))
        # print(len(embedding))
        # print(type(embedding[0]))

        response = self.opensearch_client.search(index="error-classification", body=body)
        hits = response.get("hits", {}).get("hits", [])
        # for item in hits:
        #     del item["_source"]["embedding"]
        #     print(item)

       

        if not hits:
            return 0.0, {}

        top_hit = hits[0]
        return top_hit["_score"], top_hit["_source"]

    def classify_with_claude(self, prompt_text):
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt_text
                    }
                ]
            }
        ]

        body = json.dumps({
            "messages": messages,
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 20,
            "temperature": 0.0
        })

        response = self.bedrock_client.invoke_model(
            modelId=self.claude_model_id,
            body=body,
            contentType="application/json",
            accept="application/json"
        )

        result = json.loads(response["body"].read())
        return result["content"][0]["text"].strip()

    def store_classification(self, document):
        try:
            self.opensearch_client.index(index="error-classification", body=document)
        except Exception as e:
            print(f"Failed to store classification: {e}")

    def store_log(self, log_entry):
        try:
            self.opensearch_client.index(index="logs", body=log_entry)
        except Exception as e:
            print(f"Failed to store log: {e}")

    def process_log(self, event, embedding):
        try:
            timestamp = event.get("Timestamp")
            if timestamp:
                iso_time = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z").isoformat()
            else:
                iso_time = datetime.utcnow().isoformat()

            if isinstance(embedding[0], list):
                embedding = embedding[0]

            embedding = [float(x) for x in embedding]
            assert isinstance(embedding, list)
            assert isinstance(embedding[0], float)
            assert len(embedding) == 1024

            score, nearest = self.search_opensearch(embedding)
            print(score)

            if score < 0.75:
                prompt_text = f"""
You are a system error classification assistant.
Your task is to analyze raw error log messages and classify the type of error using a broad and general category. These categories should be system-agnostic and reusable across different platforms and technologies.

Start by checking if the error matches one of the standard categories below:

- Network Issue (e.g., DNS failure, timeout, unreachable host)
- File System Permission (e.g., permission denied, read/write errors)
- Firewall/Connection Block (e.g., blocked ports, denied connections)
- Stacktrace/Crash (e.g., Java exception, segmentation fault, traceback)
- Missing Dependency or Resource (e.g., file not found, missing module)
- Authentication/Authorization Failure (e.g., invalid credentials, access denied)
- Configuration Error (e.g., invalid setting, misconfigured parameter)
- Database Error (e.g., failed query, connection refused, missing table)
- Service Down/Unavailable (e.g., 503 errors, service not responding)
- Other (only use this if nothing else fits)

If none of the above categories apply, you may define a new category, but make sure it is general and broad enough to be reused in future logs of a similar kind. Do not create overly specific categories tied to a particular system or application.
with no explanation, only the category name
Please return only the category name. for the following sentence: {event['Message']} (Status {event['Status']})
"""
                label = self.classify_with_claude(prompt_text)
                print(label)

                classification_doc = {
                    "timestamp": iso_time,
                    "message": event["Message"],
                    "embedding": embedding,
                    "category": label
                }
                self.store_classification(classification_doc)

                log_doc = {
                    "timestamp": iso_time,
                    "message": event["Message"],
                    "status": event["Status"],
                    "category": category,
                    "label_source": "LLM",
                    "filePath": event["FilePath"],
                    "hostname": event["Hostname"]
                }
                self.store_log(log_doc)

                return log_doc
            else:
                category = nearest.get("category", "uncategorized")
                print(f"Similar log found with score {score}. Category from OpenSearch: {category}")

                log_doc = {
                    "timestamp": iso_time,
                    "message": event["Message"],
                    "status": event["Status"],
                    "category": category,
                    "label_source": "opensearch",
                    "filePath": event["FilePath"],
                    "hostname": event["Hostname"]

                }
                self.store_log(log_doc)
                return log_doc

        except Exception as e:
            print(f"Error processing log: {e}")
            return {"error": str(e)}
