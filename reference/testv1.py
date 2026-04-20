import boto3
import json
from langchain_community.document_loaders import UnstructuredFileLoader

def extract_info_from_pdf(file_path):
    # 1. Load the PDF
    # Using 'elements' mode provides better structural context for complex docs
    loader = UnstructuredFileLoader(file_path, mode="elements")
    docs = loader.load()

    # Combine text content from document elements
    # Using a join ensures we send a single cohesive string to the model
    combined_text = "\n".join([doc.page_content for doc in docs])

    # 2. Setup Bedrock Converse
    client = boto3.client("bedrock-runtime", region_name="ap-southeast-2")

    # Define extraction prompt with clear JSON constraints
    prompt = f"""
    Analyze the following document and extract key information.
    Return ONLY valid JSON. Do not include markdown formatting, backticks, or any conversational text.

    Required Schema:
    {{
        "document_type": "invoice or contract",
        "summary": "brief summary of content",
        "key_entities": ["list of organizations or people mentioned"],
        "dates": ["list of relevant dates"],
        "total_amount": "total currency value if present"
    }}

    Document Content:
    {combined_text}
    """

    # 3. Call Bedrock
    response = client.converse(
        modelId="qwen.qwen3-32b-v1:0",
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ]
    )

    # 4. Clean and Parse JSON
    raw_output = response["output"]["message"]["content"][0]["text"]
    
    # Strip markdown wrappers if the model includes them despite instructions
    clean_json = raw_output.replace("```json", "").replace("```", "").strip()
    
    return json.loads(clean_json)

# Execution
if __name__ == "__main__":
    try:
        data = extract_info_from_pdf("225152.pdf")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error processing document: {e}")