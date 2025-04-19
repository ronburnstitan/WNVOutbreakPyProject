class SpatialEtl:
    def __init__(self, config):
        self.config = config
        self.remote = config.get("remote_url")
        self.local_dir = config.get("proj_dir") + "data"
        self.data_format = config.get("data_format")
        self.destination = config.get("proj_dir") + "output"


    def extract(self):
        print('Extracting data from {self.remote} to {self.local_dir}')

    def transform(self):
        print('Transforming {self.data_format}')

    def load(self):
        print('Loading data into {self.destination}')