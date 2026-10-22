import yaml


def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    with open(cfg_file, "r") as f:
        return yaml.safe_load(f)
