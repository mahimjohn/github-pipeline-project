import yaml
from src.extract.extract import extract

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

extract(config)
