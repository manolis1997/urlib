import os
import yaml

class ReadYaml:
    def __init__(self, table_name):
        self.table_name = table_name

    @property
    def read_yaml(self):
        config_path = os.path.join(os.path.dirname(__file__), "..", "..", "configs", f"{self.table_name}.yaml")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        return config
    
    def fetch_table_name(self, layer):
        return self.read_yaml.get(layer)

    @property
    def is_scd(self):
        return self.read_yaml.get("scd_type2", False)
    
    @property
    def primary_keys(self):
        return [
            col["target"]
            for col in self.read_yaml.get("columns", [])
            if col.get("primary_key") is True
        ]
    
    @property
    def non_primary_keys(self):
        non_pk_columns = [
            col["target"]
            for col in self.read_yaml.get("columns", [])
            if not col.get("primary_key", False)
        ]

        return non_pk_columns