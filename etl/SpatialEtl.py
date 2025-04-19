class SpatialEtl:
    def __init__(self, confif_dict):
        self.config_dict=confif_dict


    def extract(self):
        print(f"Extracting data from {self.config_dict.get('remote_url')} to {self.config_dict.get('proj_dir')}")

    def transform(self):
        print("Running base transform...")

    def load(self):
        print("Running base load...")
