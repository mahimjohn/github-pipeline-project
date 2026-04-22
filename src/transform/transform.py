import json
import os
import pandas as pd

def transform_commit(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    
    print("Starting commit data transformation...")
    
    all_folders = [f for f in os.listdir(bronze_path) 
               if os.path.isdir(f"{bronze_path}{f}")]
    latest_folder = sorted(all_folders)[-1]
    data_path = f"{bronze_path}{latest_folder}/huggingface_transformers/"
    
    print(f" Reading from: {data_path}")
    

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
    
    print(f"Commits transformed! {len(df_commits)} rows saved to Silver!")

def transform_pull_request(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    
    print("Starting pull request data transformation...")
    
    all_folders = [f for f in os.listdir(bronze_path) 
               if os.path.isdir(f"{bronze_path}{f}")]
    latest_folder = sorted(all_folders)[-1]
    data_path = f"{bronze_path}{latest_folder}/huggingface_transformers/"
    
    print(f" Reading from: {data_path}")
    
    with open(f"{data_path}pull_requests.json", "r") as f:
        pr_raw = json.load(f)
        
    pr_clean = []
    for pr in pr_raw:
        pr_clean.append({
            "id": pr["id"],
            "closed_at": pr["closed_at"],
            "merged_at": pr["merged_at"],
            "user_login": pr["user"]["login"],
            "state": pr["state"],
            "created_at": pr["created_at"]
        })
        
    df_pr = pd.DataFrame(pr_clean)
    
    os.makedirs(silver_path, exist_ok=True)
    df_pr.to_parquet(f"{silver_path}pull_requests.parquet", index=False)
    
    print(f" Pull requests transformed! {len(df_pr)} rows saved to Silver!")
    
def transform_issue(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    
    print("Starting issue data transformation...")
    
    all_folders = [f for f in os.listdir(bronze_path) 
               if os.path.isdir(f"{bronze_path}{f}")]
    latest_folder = sorted(all_folders)[-1]
    data_path = f"{bronze_path}{latest_folder}/huggingface_transformers/"
    
    print(f" Reading from: {data_path}")
    
    with open(f"{data_path}issues.json", "r") as f:
        issues_raw = json.load(f)
        
    issues_clean = []
    for issue in issues_raw:
        issues_clean.append({
            "id": issue["id"],
            "state": issue["state"],
            "user_login": issue["user"]["login"],
            "created_at": issue["created_at"],
            "closed_at": issue["closed_at"]
        })
        
    df_issues = pd.DataFrame(issues_clean)
    
    os.makedirs(silver_path, exist_ok=True)
    df_issues.to_parquet(f"{silver_path}issues.parquet", index=False)
    
    print(f" Issues transformed! {len(df_issues)} rows saved to Silver!")
    
def transform_contributor(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    
    print("Starting contributor data transformation...")
    
    all_folders = [f for f in os.listdir(bronze_path) 
               if os.path.isdir(f"{bronze_path}{f}")]
    latest_folder = sorted(all_folders)[-1]
    data_path = f"{bronze_path}{latest_folder}/huggingface_transformers/"
    
    print(f" Reading from: {data_path}")
    
    with open(f"{data_path}contributors.json", "r") as f:
        contributors_raw = json.load(f)
        
    contributors_clean = []
    for contributor in contributors_raw:
        contributors_clean.append({
            "login": contributor["login"],
            "contributions": contributor["contributions"]
        })
        
    df_contributors = pd.DataFrame(contributors_clean)
    
    os.makedirs(silver_path, exist_ok=True)
    df_contributors.to_parquet(f"{silver_path}contributors.parquet", index=False)
    
    print(f" Contributors transformed! {len(df_contributors)} rows saved to Silver!")
    
def transform(config):
    transform_commit(config)
    transform_pull_request(config)
    transform_issue(config)
    transform_contributor(config)
    
    print("All transformations done!")