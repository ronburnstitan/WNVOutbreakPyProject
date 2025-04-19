import sys
import os
import logging

sys.path.append(r"C:\Users\rburn\PycharmProjects\WNVOutbreakPyProject")

import arcpy
from etl.GSheetsEtl import GSheetsEtl
from config.config_utils import load_config


def etl(config):
    logging.debug("Entering etl()")
    logging.info("Start etl process...")
    etl_instance = GSheetsEtl(config)
    etl_instance.process()
    logging.debug("Exiting etl()")


def buffer_layer(input_fc, buffer_distance, output_name):
    logging.debug(f"Entering buffer_layer() for {input_fc}")
    logging.info(f"Buffering {input_fc} by {buffer_distance} feet...")

    if arcpy.Exists(output_name):
        logging.info(f"{output_name} already exists. Deleting it.")
        arcpy.management.Delete(output_name)

    arcpy.Buffer_analysis(
        in_features=input_fc,
        out_feature_class=output_name,
        buffer_distance_or_field=f"{buffer_distance} Feet",
        line_side="FULL",
        line_end_type="ROUND",
        dissolve_option="ALL"
    )
    logging.debug(f"Exiting buffer_layer() for {input_fc}")


def intersect_buffers(buffer_list, output_name):
    logging.debug("Entering intersect_buffers()")
    logging.info(f"Intersecting buffers into {output_name}...")
    arcpy.Intersect_analysis(
        in_features=buffer_list,
        out_feature_class=output_name
    )
    logging.debug("Exiting intersect_buffers()")


def spatial_join(address_fc, intersect_fc, output_name):
    logging.debug("Entering spatial_join()")
    logging.info(f"Performing spatial join of {address_fc} with {intersect_fc}...")
    arcpy.SpatialJoin_analysis(
        target_features=address_fc,
        join_features=intersect_fc,
        out_feature_class=output_name,
        join_type="KEEP_COMMON"
    )
    logging.debug("Exiting spatial_join()")


def erase_avoid_areas(intersect_fc, avoid_buffer_fc, output_fc):
    logging.debug("Entering erase_avoid_areas()")
    logging.info(f"Erasing {avoid_buffer_fc} from {intersect_fc} to create {output_fc}...")
    arcpy.Erase_analysis(
        in_features=intersect_fc,
        erase_features=avoid_buffer_fc,
        out_feature_class=output_fc
    )
    logging.debug("Exiting erase_avoid_areas()")


def count_at_risk(joined_fc):
    logging.debug("Entering count_at_risk()")
    count = int(arcpy.GetCount_management(joined_fc)[0])
    logging.info(f"Number of addresses at risk: {count}")
    logging.debug("Exiting count_at_risk()")


def exportMap(config):
    logging.debug("Entering exportMap()")

    aprx = arcpy.mp.ArcGISProject(f"{config.get('proj_dir')}WestNileOutbreak.aprx")
    layout = aprx.listLayouts()[0]

    subtitle = input("Enter the subtitle for the map: ")

    for elm in layout.listElements("TEXT_ELEMENT"):
        if elm.name == "Title":
            elm.text = f"West Nile Virus Outbreak – {subtitle}"

    export_path = f"{config.get('proj_dir')}WNV_Map_{subtitle.replace(' ', '_')}.pdf"
    layout.exportToPDF(export_path)
    logging.info(f"Map exported to: {export_path}")

    logging.debug("Exiting exportMap()")


def main():
    config = load_config()

    logging.basicConfig(
        filename=f"{config.get('proj_dir')}wnv.log",
        filemode="w",
        level=logging.DEBUG
    )

    logging.info("Starting West Nile Virus Simulation")

    arcpy.env.workspace = f"{config.get('proj_dir')}WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    etl(config)

    avoid_buffer = "avoid_points_buffer"
    buffer_layer("avoid_points", 1500, avoid_buffer)

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

    intersect_output = "HighRisk_Intersect"
    intersect_buffers(buffer_outputs, intersect_output)

    sprayed_area = "Spray_Eligible_Area"
    erase_avoid_areas(intersect_output, avoid_buffer, sprayed_area)

    address_fc = "Addresses"
    joined_output = "Joined_Addresses"
    spatial_join(address_fc, sprayed_area, joined_output)

    count_at_risk(joined_output)

    exportMap(config)


if __name__ == "__main__":
    main()
