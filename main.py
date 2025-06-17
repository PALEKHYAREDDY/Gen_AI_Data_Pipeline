import requests
import json

def ask_ollama(prompt, model="mistral"):
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": model, "prompt": prompt},
        stream=True
    )

    full_response = ""
    for line in response.iter_lines():
        if line:
            try:
                chunk = json.loads(line.decode("utf-8"))
                full_response += chunk.get("response", "")
            except Exception as e:
                print(f"Streaming error: {e}")
    return full_response
print(ask_ollama("What is the capital of France?"))
