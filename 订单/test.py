import requests
import json

def stream_chat(server_ip, prompt):
    url = f"http://{server_ip}:11434/api/generate"
    
    data = {
        "model": "deepseek-r1:7b",
        "prompt": prompt,
        "stream": True
    }
    
    response = requests.post(url, json=data, stream=True)
    print("AI: ", end="", flush=True)
    for line in response.iter_lines():
        if line:
            try:
                json_data = json.loads(line)
                if 'response' in json_data:
                    print(json_data['response'], end="", flush=True)
            except:
                pass
    print()

# 使用示例
if __name__ == "__main__":
    server_ip = "10.10.8.95"
    
    while True:
        user_input = input("\n你: ").strip()
        if user_input.lower() in ['quit', '退出']:
            break
        stream_chat(server_ip, user_input)