import boto3
import json
import time

def classify_error(prompt_text):
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

    # Construct the conversation messages
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

    # Define the model ID for Claude 3.5 Sonnet v2
    model_id = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"

    # Prepare the request body
    body = json.dumps({
        "messages": messages,
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 20,
        "temperature": 0.0
    })

    # Invoke the model
    start_time = time.time()
    response = bedrock.invoke_model(
        modelId=model_id,
        body=body,
        contentType="application/json",
        accept="application/json"
    )
    end_time = time.time()

    # Parse the response
    result = json.loads(response["body"].read())
    output_text = result["content"][0]["text"].strip()

    return output_text, end_time - start_time

if __name__ == "__main__":
    prompt = "Return only the error category name for: 'Access denied while opening file.' (Status 403)"
    category, duration = classify_error(prompt)
    print("Model Response:")
    print(category or "[Empty]")
    print(f"⏱️ Response Time: {duration:.2f} seconds")
