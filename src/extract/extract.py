import requests as r 
import json
import os
from datetime import datetime

token = os.environ.get("GITHUB_TOKEN")

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github+json"
}

def extract(config):
    owner = config["repository"]["owner"]
    repo = config["repository"]["repo"]
    base_url = config["github"]["base_url"]
    bronze_path = config["paths"]["bronze"]
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    save_folder = f"{bronze_path}{timestamp}/{owner}_{repo}/"
    os.makedirs(save_folder, exist_ok=True)
    
    endpoints = config["endpoints"]
    
    for endpoint_name, endpoint_info in endpoints.items():
        path = endpoint_info["path"]
        path = path.replace("{owner}", owner).replace("{repo}", repo)
        url = base_url + path
        
        print(f"📥 Fetching {endpoint_name} from {url}")
        response = r.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            with open(f"{save_folder}{endpoint_name}.json", "w") as f:
                json.dump(data, f, indent=4)
            print(f"✅ {endpoint_name} saved successfully.")
        else:
            print(f"❌ Failed to fetch {endpoint_name}. Status code: {response.status_code}")
            