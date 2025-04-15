import boto3
import json
import time

def test_deepseek(prompt_text):
    bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

    # ✅ Send prompt as-is, no special chat tokens
    formatted_prompt = prompt_text

    payload = {
        "prompt": formatted_prompt,
        "temperature": 0.2,
        "top_p": 0.9,
        "max_tokens": 50  # Reduced for single-word response
    }

    start_time = time.time()  # ⏱️ Start timer

    response = bedrock_client.invoke_model(
        modelId="us.deepseek.r1-v1:0",  # or full ARN if required
        body=json.dumps(payload),
        contentType="application/json",
        accept="application/json"
    )

    end_time = time.time()  # ⏱️ End timer
    duration = end_time - start_time

    result = json.loads(response["body"].read())
    completions = result.get("completions") or result.get("choices")

    if not completions:
        print("⚠️ No completions found. Raw response:")
        print(json.dumps(result, indent=2))
        return "", duration

    # Grab and clean result
    text = completions[0].get("data", {}).get("text", "") or completions[0].get("text", "")
    print(text)
    return text.strip(), duration

if __name__ == "__main__":
    prompt = "Only return the category name. No explanation.: 'Access denied while opening file.' (Status 403)"
    response_text, elapsed = test_deepseek(prompt)

    print("Model Response:")
    print(response_text or "[Empty]")
    print(f"⏱️ Response Time: {elapsed:.2f} seconds")
