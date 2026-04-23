import yaml
from src.extract.extract import extract
from src.validate.validate import validate_bronze ,validate_silver
from src.transform.transform import transform
# from src.load.load import load


with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

extract(config)

bronze_ok = validate_bronze(config)

if not bronze_ok:
    print("Bronze validation failed. Please fix the issues and try again.")
    exit()
    

transform(config)

silver_ok = validate_silver(config)

if not silver_ok:
    print("Silver validation failed. Please fix the issues and try again.")
    exit()
    
# load(config)
    
