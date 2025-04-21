from embedding_generator import EmbeddingGenerator
from error_classifier import ErrorClassifier
from kafka import KafkaConsumer
import json

# from kafka import KafkaConsumer
import json



class LogProcessor:
    def __init__(self, opensearch_host, region):
        self.classifier = ErrorClassifier(opensearch_host, region)
        self.embedder = EmbeddingGenerator(region)

    def format_log_for_embedding(self, event):
        return f"Status {event['Status']}: {event['Message']} "

    def process(self, event):
        # Step 1: Check similarity using ErrorClassifier (requires embedding first)
        formatted_text = self.format_log_for_embedding(event)
        embedding = self.embedder.get_embedding(formatted_text)

        # Step 2: Classifier decides whether to use Claude or reuse existing category
        result = self.classifier.process_log(event, embedding)
        return result

if __name__ == "__main__":
    sample_event = {
    "Format": "nginx",
    "Timestamp": "11/Feb/2024:16:40:50 +0000",
    "IP": "192.168.1.200",
    "Status": 0,
    "User": "user1",
    "Request": "POST /api/data HTTP/1.1",
    "User-Agent": "Mozilla/5.0 (Linux)",
    "Message": "App crashed due to malformed config.json"
    }

    processor = LogProcessor(opensearch_host="d48fb9qgyoz01pr1jbfk.us-east-1.aoss.amazonaws.com", region="us-east-1")
    classification = processor.process(sample_event)
    print("Final classification result:", classification)



