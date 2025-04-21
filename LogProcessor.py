from confluent_kafka import Consumer, KafkaException, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from embedding_generator import EmbeddingGenerator
from error_classifier import ErrorClassifier
import json
import sys


class LogProcessor:
    def __init__(self, opensearch_host, region):
        self.classifier = ErrorClassifier(opensearch_host, region)
        self.embedder = EmbeddingGenerator(region)

    def format_log_for_embedding(self, event):
        return f"{event['Message']}"

    def process(self, event):
        formatted_text = self.format_log_for_embedding(event)
        embedding = self.embedder.get_embedding(formatted_text)
        result = self.classifier.process_log(event, embedding)
        return result
    
# if __name__ == "__main__":
#     sample_event = {
#     "Format": "nginx",
#     "Timestamp": "11/Feb/2024:16:40:50 +0000",
#     "IP": "192.168.1.200",
#     "Status": 0,
#     "User": "user1",
#     "Request": "POST /api/data HTTP/1.1",
#     "User-Agent": "Mozilla/5.0 (Linux)",
#     "Message": "Connection refused: connect to 11.4.6.7",
#     "FilePath": "",
#     "Hostname": ""
#     }

#     processor = LogProcessor(opensearch_host="d48fb9qgyoz01pr1jbfk.us-east-1.aoss.amazonaws.com", region="us-east-1")
#     classification = processor.process(sample_event)
#     print("Final classification result:", classification)


def oauth_cb(oauth_config):
    token, expiry = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    return token, expiry / 1000  # Convert milliseconds to seconds


def error_cb(err):
    print(f"[Kafka Error] {err}")
    if err.code() == KafkaError._ALL_BROKERS_DOWN:
        print("❌ All Kafka brokers are down. Exiting.")
        sys.exit(1)


consumer_conf = {
    'bootstrap.servers': 'b-2-public.kafkacluster.f176qg.c2.kafka.us-east-1.amazonaws.com:9198',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
    'group.id': 'log-processor',
    'auto.offset.reset': 'earliest',
    'error_cb': error_cb
}

consumer = Consumer(consumer_conf)

try:

    consumer.subscribe(['logs_parsed'])
    print("Subscribed to topic 'logs_parsed'")

    processor = LogProcessor(
        opensearch_host="d48fb9qgyoz01pr1jbfk.us-east-1.aoss.amazonaws.com",
        region="us-east-1"
    )

    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"❌ Topic not found: {msg.error()}")
                sys.exit(1)
            else:
                print(f"[Message Error] {msg.error()}")
                continue

        try:
            event = json.loads(msg.value().decode('utf-8'))

            # print(msg.value().decode('utf-8'))
            classification = processor.process(event)
            print("✔️ Final classification result:", classification)
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
        except Exception as e:
            print(f"❌ Processing error: {e}")

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    consumer.close()
    print("🔒 Kafka consumer closed.")
