import json
import os
import pandas as pd

def transform(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    
    print("🧹 Starting transformation...")
    
    all_folders = os.listdir(bronze_path)
    latest_folder = sorted(all_folders)[-1]
    data_path = f"{bronze_path}{latest_folder}/huggingface_transformers/"
    
    print(f"📂 Reading from: {data_path}")
    
    # Read commits.json
    with open(f"{data_path}commits.json", "r") as f:
        commits_raw = json.load(f)
    
    commits_clean = []
    for commit in commits_raw:
        commits_clean.append({
            "sha": commit["sha"],
            "message": commit["commit"]["message"],
            "author_name": commit["commit"]["author"]["name"],
            "date": commit["commit"]["author"]["date"]
        })
    
    df_commits = pd.DataFrame(commits_clean)
    
    os.makedirs(silver_path, exist_ok=True)
    df_commits.to_parquet(f"{silver_path}commits.parquet", index=False)
    
    print(f"✅ Commits transformed! {len(df_commits)} rows saved to Silver!")