from embedding_generator import EmbeddingGenerator
from error_classifier import ErrorClassifier

class LogProcessor:
    def __init__(self, opensearch_host, region):
        self.classifier = ErrorClassifier(opensearch_host, region)
        self.embedder = EmbeddingGenerator(region)

    def format_log_for_embedding(self, event):
        return f"Status {event['status']} from {event['source']}: {event['message']} [Category: {event.get('category', 'unknown')}]"

    def process(self, event):
        # Step 1: Check similarity using ErrorClassifier (requires embedding first)
        formatted_text = self.format_log_for_embedding(event)
        embedding = self.embedder.get_embedding(formatted_text)

        # Step 2: Classifier decides whether to use Claude or reuse existing category
        result = self.classifier.process_log(event, embedding)
        return result

if __name__ == "__main__":
    sample_event = {
        "timestamp": "2025-04-13T18:00:00Z",
        "message": "Timeout occurred while processing order 12345",
        "status": 500,
        "category": "error",
        "source": "order-service"
    }

    processor = LogProcessor(opensearch_host="akoanqav21wglrnpiu4b.eu-north-1.aoss.amazonaws.com", region="eu-north-1")
    classification = processor.process(sample_event)
    print("Final classification result:", classification)
