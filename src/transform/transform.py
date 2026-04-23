import json
import os
import pandas as pd
from datetime import datetime

def get_paths(config):
    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    return bronze_path, silver_path

def get_latest_data_path(bronze_path):
    all_folders = [
        f for f in os.listdir(bronze_path)
        if os.path.isdir(os.path.join(bronze_path, f))
    ]
    latest_folder = sorted(all_folders)[-1]
    return os.path.join(bronze_path, latest_folder, "huggingface_transformers")

def get_silver_output_path(silver_path):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = os.path.join(silver_path, timestamp) + "/"
    os.makedirs(output_path, exist_ok=True)
    return output_path

def transform_commit(config, output_path):
    bronze_path, silver_path = get_paths(config)
    print("Starting commit data transformation")
    data_path = get_latest_data_path(bronze_path)
    print(f" Reading from: {data_path}")
    with open(os.path.join(data_path, "commits.json"), "r") as f:
        commits_raw = json.load(f)
    commits_clean = []
    for commit in commits_raw:
        commits_clean.append({
            "id": commit["sha"],
            "message": commit["commit"]["message"],
            "author_name": commit["commit"]["author"]["name"],
            "date": commit["commit"]["author"]["date"]
        })
    df_commits = pd.DataFrame(commits_clean)
    df_commits.to_parquet(f"{output_path}commits.parquet", index=False)
    print(f"Commits transformed! {len(df_commits)} rows saved to Silver!")

def transform_pull_request(config, output_path):
    bronze_path, silver_path = get_paths(config)
    print("Starting pull request data transformation")
    data_path = get_latest_data_path(bronze_path)
    print(f" Reading from: {data_path}")
    with open(os.path.join(data_path, "pull_requests.json"), "r") as f:
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
    df_pr.to_parquet(f"{output_path}pull_requests.parquet", index=False)
    print(f" Pull requests transformed! {len(df_pr)} rows saved to Silver!")

def transform_issue(config, output_path):
    bronze_path, silver_path = get_paths(config)
    print("Starting issue data transformation")
    data_path = get_latest_data_path(bronze_path)
    print(f" Reading from: {data_path}")
    with open(os.path.join(data_path, "issues.json"), "r") as f:
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
    df_issues.to_parquet(f"{output_path}issues.parquet", index=False)
    print(f" Issues transformed! {len(df_issues)} rows saved to Silver!")

def transform_contributor(config, output_path):
    bronze_path, silver_path = get_paths(config)
    print("Starting contributor data transformation")
    data_path = get_latest_data_path(bronze_path)
    print(f" Reading from: {data_path}")
    with open(os.path.join(data_path, "contributors.json"), "r") as f:
        contributors_raw = json.load(f)
    contributors_clean = []
    for contributor in contributors_raw:
        contributors_clean.append({
            "login": contributor["login"],
            "contributions": contributor["contributions"]
        })
    df_contributors = pd.DataFrame(contributors_clean)
    df_contributors.to_parquet(f"{output_path}contributors.parquet", index=False)
    print(f" Contributors transformed! {len(df_contributors)} rows saved to Silver!")

def transform(config):
    bronze_path, silver_path = get_paths(config)
    data_path = get_latest_data_path(bronze_path)
    output_path = get_silver_output_path(silver_path)
    transform_commit(config, output_path)
    transform_pull_request(config, output_path)
    transform_issue(config, output_path)
    transform_contributor(config, output_path)
    print("All transformations done")