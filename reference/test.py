import boto3

client = boto3.client("bedrock-runtime", region_name="ap-southeast-2")

response = client.converse(
    modelId="qwen.qwen3-32b-v1:0",
    messages=[
        {
            "role": "user",
            "content": [
                {"text": "hi"}
            ]
        }
    ]
)

print(response["output"]["message"]["content"][0]["text"])