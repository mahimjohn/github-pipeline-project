import os
import json
import pandas as pd

def validate_bronze(config):
    bronze_path = config["paths"]["bronze"]
    
    print("Starting the validation process of the bronze layer...")
    
    folders = os.listdir(bronze_path)
    all_folders = sorted(folders)[-1]
    data_path = f"{bronze_path}{all_folders}/huggingface_transformers/"
    
    expected_files = {
        "commits.json":       ["sha", "commit", "author"],
        "pull_requests.json": ["id", "state", "user", "created_at", "closed_at", "merged_at"],
        "issues.json":        ["id", "state", "user", "created_at", "closed_at"],
        "contributors.json":  ["login", "contributions"]
    }
    
    all_passed = True
    
    for filename, expected_fields in expected_files.items():
        file_path = f"{data_path}{filename}"
        
        if not os.path.exists(file_path):
            print(f"{filename} is not found")
            all_passed = False
            continue
        
        if not os.path.getsize(file_path) == 0:
            print(f"{filename} is empty")
            all_passed = False
            continue
        
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                
            if len(data) == 0:
                print(f"{filename} has no records")
            
            for field in expected_fields:
                if field not in data[0]:
                    print(f"{filename} is missing expected field: {field}")
                    all_passed = False
                    
            print(f"{filename} passed validation.")
        
        except json.JSONDecodeError:
            print(f"Failed! {filename} is corrupt.")
            all_passed = False
            
    if all_passed:
        print("validation check completed!")
    else:
        print("validation check failed! Please check the above errors.")
        
    return all_passed

