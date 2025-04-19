import requests
from etl.SpatialEtl import SpatialEtl

class GSheetEtl(SpatialEtl):
    def __init__(self, remote, local_dir, data_format, destination):
        super().__init__(remote, local_dir, data_format, destination)

    def extract(self):
        print("Extracting from Google Sheets...")

        response = requests.get(self.remote)
        if response.status_code == 200:
            output_path = f"{self.local_dir}/google_sheet_data.csv"
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"Data written to {output_path}")
        else:
            print(f"Failed to download data. Status code: {response.status_code}")

    def process(self):
        self.extract()
        super().transform()
        super().load()