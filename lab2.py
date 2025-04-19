import sys
import os

sys.path.append(r"C:\Users\rburn\PycharmProjects\WNVOutbreakPyProject")

import arcpy
from etl.GSheetsEtl import GSheetsEtl
from config.config_utils import load_config


def etl(config):
    print("Start etl process...")
    etl_instance = GSheetsEtl(config)
    etl_instance.process()


def buffer_layer(input_fc, buffer_distance, output_name):
    print(f"Buffering {input_fc} by {buffer_distance} feet...")

    if arcpy.Exists(output_name):
        print(f"{output_name} already exists. Deleting it.")
        arcpy.management.Delete(output_name)

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


def erase_avoid_areas(intersect_fc, avoid_buffer_fc, output_fc):
    print(f"Erasing {avoid_buffer_fc} from {intersect_fc} to create {output_fc}...")
    arcpy.Erase_analysis(
        in_features=intersect_fc,
        erase_features=avoid_buffer_fc,
        out_feature_class=output_fc
    )


def count_at_risk(joined_fc):
    count = int(arcpy.GetCount_management(joined_fc)[0])
    print(f"Number of addresses at risk: {count}")


def main():
    config = load_config()
    arcpy.env.workspace = f"{config.get('proj_dir')}WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    # Run ETL to get fresh avoid_points
    etl(config)

    # Buffer avoid_points layer
    avoid_buffer = "avoid_points_buffer"
    buffer_layer("avoid_points", 1500, avoid_buffer)

    # Buffer all high-risk layers
    layers = {
        "Mosquito_Larval_Sites": 1500,
        "Wetlands": 1500,
        "Lakes_and_Reservoirs___Boulder_County": 1500,
        "OSMP_Properties": 1500
    }

    buffer_outputs = []

    for layer, dist in layers.items():
        output_name = f"{layer}_buffer"
        buffer_layer(layer, dist, output_name)
        buffer_outputs.append(output_name)

    # Intersect high-risk buffers
    intersect_output = "HighRisk_Intersect"
    intersect_buffers(buffer_outputs, intersect_output)

    # Erase avoid buffer from high-risk zones
    sprayed_area = "Spray_Eligible_Area"
    erase_avoid_areas(intersect_output, avoid_buffer, sprayed_area)

    # Join addresses with spray-eligible area to identify people at risk
    address_fc = "Addresses"
    joined_output = "Joined_Addresses"
    spatial_join(address_fc, sprayed_area, joined_output)

    count_at_risk(joined_output)


if __name__ == "__main__":
    main()
