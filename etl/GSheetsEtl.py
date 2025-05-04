import requests
import arcpy
import csv
import shutil
from etl.SpatialEtl import SpatialEtl


class GSheetsEtl(SpatialEtl):
    """
        GSheetsEtl extends the SpatialEtl class to implement an ETL process specifically for
        extracting address data from a Google Sheet, transforming the address format for
        geocoding, and loading the results into a geodatabase.

        This class automates:
            1. Downloading CSV data from a public Google Sheets link.
            2. Adding a 'SingleLine' field for geocoding.
            3. Performing address geocoding using an online locator service.
            4. Saving geocoded features to a geodatabase.

        Attributes:
            config_dict (dict): Dictionary containing paths, URLs, and configuration parameters.
        """

    def __init__(self, config_dict):
        """
                Initialize the GSheetsEtl class.

                Args:
                    config_dict (dict): A configuration dictionary that includes:
                        - 'remote_url': URL to the public Google Sheet as CSV.
                        - 'proj_dir': Path to the local project directory.
        """
        self.config_dict = config_dict
        super().__init__(config_dict)

    def extract(self):
        """
                Downloads a CSV file from a public Google Sheets link and saves it locally.
                The file is named 'addresses.csv' and stored in the specified project directory.
        """
        print("Extracting from Google Sheets...")
        remote_url = self.config_dict.get('remote_url')
        local_csv_path = f"{self.config_dict.get('proj_dir')}addresses.csv"

        r = requests.get(remote_url)
        r.encoding = "utf-8"

        if r.status_code == 200:
            with open(local_csv_path, "w", encoding="utf-8") as output_file:
                output_file.write(r.text)
            print(f"Data saved to {local_csv_path}")
        else:
            print(f"Failed to download data. Status code: {r.status_code}")

    def transform(self):
        """
                Adds a 'SingleLine' field to each address row by combining street and zip code.
                This prepares the data for batch geocoding.
                The original CSV is overwritten with the transformed version.
        """
        print("Running base transform...")
        input_csv = f"{self.config_dict.get('proj_dir')}addresses.csv"
        temp_csv = f"{self.config_dict.get('proj_dir')}addresses_transformed.csv"

        with open(input_csv, mode="r", encoding="utf-8") as infile, open(temp_csv, mode="w", newline="", encoding="utf-8") as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + ["SingleLine"]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                street = row.get("Street Address", "").strip()
                zipcode = row.get("ZipCode", "").strip()
                row["SingleLine"] = f"{street}, Boulder CO {zipcode}"
                writer.writerow(row)

        shutil.move(temp_csv, input_csv)
        print("SingleLine addresses added.")

        # ✅ Debug: print a sample row
        with open(input_csv, mode="r", encoding="utf-8") as f:
            preview = next(csv.DictReader(f))
            print("Sample row to be geocoded:", preview)

    def load(self):
        """
                Geocodes the addresses using the ArcGIS World Geocoding Service.
                Outputs are saved to a feature class in the project geodatabase.
                A backup copy is made to 'avoid_points'.
        """
        print("Running base load...")

        arcpy.env.workspace = f"{self.config_dict.get('proj_dir')}WestNileOutbreak.gdb"
        arcpy.env.overwriteOutput = True

        in_table = f"{self.config_dict.get('proj_dir')}addresses.csv"
        geocoded_output = "geocoded_addresses"
        locator_url = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer"

        try:
            print("Checking fields in CSV before geocoding...")
            fields = [f.name for f in arcpy.ListFields(in_table)]
            print("Fields:", fields)

            if "SingleLine" not in fields:
                raise ValueError("Field 'SingleLine' not found in CSV. Make sure transform step added it correctly.")

            print("Geocoding addresses...")
            arcpy.geocoding.GeocodeAddresses(
                in_table=in_table,
                address_locator=locator_url,
                in_address_fields="SingleLine SingleLine",
                out_feature_class=geocoded_output
            )
            print(f"Created geocoded feature class: {geocoded_output}")

            arcpy.management.CopyFeatures(geocoded_output, "avoid_points")
            print("Copied to 'avoid_points' in geodatabase")

        except Exception as e:
            print(f"Geocoding failed: {e}")

    def process(self):
        """
                Executes the full ETL pipeline: extract, transform, and load.
        """
        self.extract()
        self.transform()
        self.load()
