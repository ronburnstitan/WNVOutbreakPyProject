import yaml
import os

def load_config(path=None):
    if not path:
        path = os.path.join(os.path.dirname(__file__), "wnvoutbreak.yaml")

    with open(path, "r") as f:
        config_dict = yaml.safe_load(f)

    return config_dict
