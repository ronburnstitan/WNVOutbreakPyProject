import sys
import os

sys.path.append(r"C:\Users\rburn\PycharmProjects\WNVOutbreakPyProject")

import arcpy
from config.config_utils import load_config
from etl.GSheetEtl import GSheetEtl


arcpy.env.workspace = r"C:\Users\rburn\Documents\APPS305\WestNileOutbreak\WestNileOutbreak.gdb"
arcpy.env.overwriteOutput = True

def etl():
    print("Start etl process...")
    config = load_config()
    etl_instance = GSheetsEtl(config)
    etl_instance.process()

def buffer_layer(input_fc, buffer_distance, output_name):
    print(f"Buffering {input_fc} by {buffer_distance} feet...")
    arcpy.Buffer_analysis(
        in_features=input_fc,
        out_feature_class=output_name,
        buffer_distance_or_field=f"{buffer_distance} Feet",
        line_side="FULL",
        line_end_type="ROUND",
        dissolve_option="ALL"
    )

def intersect_buffers(buffer_list, output_name):
    print(f"Intersecting buffers into {output_name}...")
    arcpy.Intersect_analysis(
        in_features=buffer_list,
        out_feature_class=output_name
    )

def spatial_join(address_fc, intersect_fc, output_name):
    print(f"Performing spatial join of {address_fc} with {intersect_fc}...")
    arcpy.SpatialJoin_analysis(
        target_features=address_fc,
        join_features=intersect_fc,
        out_feature_class=output_name,
        join_type="KEEP_COMMON"
    )

# def add_to_project(layer_path):
#     aprx = arcpy.mp.ArcGISProject("CURRENT")
#     map_doc = aprx.listMaps()[0]
#     map_doc.addDataFromPath(layer_path)
#     aprx.save()

def count_at_risk(joined_fc):
    count = int(arcpy.GetCount_management(joined_fc)[0])
    print(f"Number of addresses at risk: {count}")

def main():
    layers = {
        "Mosquito_Larval_Sites": "",
        "Wetlands": "",
        "Lakes_and_Reservoirs___Boulder_County": "",
        "OSMP_Properties": ""
    }

    buffer_outputs = []

    for layer in layers:
        buffer_dist = input(f"Enter buffer distance in feet for {layer}: ")
        output_name = f"{layer}_buffer"
        buffer_layer(layer, buffer_dist, output_name)
        buffer_outputs.append(output_name)

    intersect_output = "HighRisk_Intersect"
    intersect_buffers(buffer_outputs, intersect_output)
    #add_to_project(f"{arcpy.env.workspace}\\{intersect_output}")

    address_fc = "Addresses"
    joined_output = "Joined_Addresses"
    spatial_join(address_fc, intersect_output, joined_output)
    #add_to_project(f"{arcpy.env.workspace}\\{joined_output}")

    count_at_risk(joined_output)

if __name__ == "__main__":
    main()
