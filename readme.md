# West Nile Virus Outbreak GIS Project

## Overview

This project identifies Boulder addresses that fall within designated pesticide spraying zones considered safe for mosquito control. It integrates multiple GIS layers including mosquito larval sites, wetlands, water bodies, and sensitive individual locations to determine final spray areas while avoiding harm to those with sensitivities.

## Project Goals

- Buffer multiple mosquito risk layers to identify high-risk zones
- Buffer address points of sensitive individuals to exclude them from treatment areas
- Use spatial analysis (intersect and erase) to isolate safe spray areas
- Join address data to the safe spray zones to identify targets
- Render and filter map layers using symbology and definition queries
- Export the results to a map and CSV file

## Features

- Loads address data from Google Sheets using a custom ETL process
- Buffers multiple mosquito risk layers
- Buffers around sensitive individual addresses
- Uses spatial intersect and erase tools to determine safe spray zones
- Performs spatial joins to identify affected addresses
- Applies definition queries and renders maps
- Exports address data to CSV
- Generates a final PDF map layout

## Requirements

- ArcGIS Pro (Python 3.7+ with arcpy and arcpy.mp)
- Internet access to fetch Google Sheet data
- Project `.aprx` file: `WestNileOutbreak.aprx`
- `WestNileOutbreak.gdb` geodatabase

## How to Run

1. Set up your environment in ArcGIS Pro (Python 3, arcpy installed).
2. Clone this repo or download the project files.
3. Configure the `config.yaml` file to point to your working directory and remote URL for the address sheet.
4. Run the script:

```bash
python finalproject.py
