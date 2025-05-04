import sys
import os
import logging
import csv
import arcpy

sys.path.append(r"C:\Users\rburn\PycharmProjects\WNVOutbreakPyProject")

from etl.GSheetsEtl import GSheetsEtl
from config.config_utils import load_config


def etl(config):
    """
        Executes the full ETL process using the GSheetsEtl class.

        Args:
            config (dict): Configuration dictionary with project paths and URLs.
    """
    try:
        logging.debug("Entering etl()")
        logging.info("Start etl process...")
        etl_instance = GSheetsEtl(config)
        etl_instance.process()
        logging.debug("Exiting etl()")
    except Exception as e:
        logging.error(f"Error in etl: {e}")


def buffer_layer(input_fc, buffer_distance, output_name):
    """
        Creates a buffer around an input feature class.

        Args:
            input_fc (str): Input feature class to buffer.
            buffer_distance (float): Distance in feet for buffering.
            output_name (str): Name for the resulting buffer output feature class.
    """
    try:
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
    except Exception as e:
        logging.error(f"Error in buffer_layer: {e}")


def intersect_buffers(buffer_list, output_name):
    """
        Intersects multiple buffered feature classes into a single output.

        Args:
            buffer_list (list): List of buffered feature class names.
            output_name (str): Output feature class name for intersect result.
    """
    try:
        logging.debug("Entering intersect_buffers()")
        logging.info(f"Intersecting buffers into {output_name}...")
        arcpy.Intersect_analysis(
            in_features=buffer_list,
            out_feature_class=output_name
        )
        logging.debug("Exiting intersect_buffers()")
    except Exception as e:
        logging.error(f"Error in intersect_buffers: {e}")


def erase_avoid_areas(intersect_fc, avoid_buffer_fc, output_fc):
    """
        Erases sensitive areas (avoid points) from high-risk spray zones.

        Args:
            intersect_fc (str): Input feature class of intersected high-risk areas.
            avoid_buffer_fc (str): Buffered feature class of sensitive areas.
            output_fc (str): Output feature class for the spray-eligible zone.
    """
    try:
        logging.debug("Entering erase_avoid_areas()")
        logging.info(f"Erasing {avoid_buffer_fc} from {intersect_fc} to create {output_fc}...")
        arcpy.Erase_analysis(
            in_features=intersect_fc,
            erase_features=avoid_buffer_fc,
            out_feature_class=output_fc
        )
        logging.debug("Exiting erase_avoid_areas()")
    except Exception as e:
        logging.error(f"Error in erase_avoid_areas: {e}")


def spatial_join(address_fc, join_fc, output_fc):
    """
        Performs a spatial join between addresses and spray-eligible areas.

        Args:
            address_fc (str): Feature class of addresses.
            join_fc (str): Feature class of spray zones.
            output_fc (str): Output feature class for joined results.
    """
    try:
        logging.debug("Entering spatial_join()")
        logging.info(f"Performing spatial join of {address_fc} with {join_fc}...")
        arcpy.SpatialJoin_analysis(
            target_features=address_fc,
            join_features=join_fc,
            out_feature_class=output_fc,
            join_type="KEEP_COMMON"
        )
        logging.debug("Exiting spatial_join()")
    except Exception as e:
        logging.error(f"Error in spatial_join: {e}")


def export_addresses_to_csv(fc, csv_path):
    """
       Exports address data from a feature class to a CSV file.

       Args:
           fc (str): Input feature class with address data.
           csv_path (str): Full path to the output CSV file.
    """
    try:
        fields = ["StreetAddress"]
        with open(csv_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(fields)
            with arcpy.da.SearchCursor(fc, fields) as cursor:
                for row in cursor:
                    writer.writerow(row)
        logging.info(f"Addresses exported to {csv_path}")
    except Exception as e:
        logging.error(f"Error in export_addresses_to_csv: {e}")


def count_at_risk(joined_fc):
    """
        Counts how many addresses fall within the spray-eligible area.

        Args:
            joined_fc (str): Feature class resulting from spatial join.
    """
    try:
        logging.debug("Entering count_at_risk()")
        count = int(arcpy.GetCount_management(joined_fc)[0])
        logging.info(f"Number of addresses at risk: {count}")
        logging.debug("Exiting count_at_risk()")
    except Exception as e:
        logging.error(f"Error in count_at_risk: {e}")


def set_spatial_reference():
    """
        Sets the map’s spatial reference to NAD 1983 StatePlane Colorado North (EPSG 2231).
    """
    try:
        logging.debug("Entering set_spatial_reference()")
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        map_doc = aprx.listMaps()[0]
        map_doc.defaultSpatialReference = arcpy.SpatialReference(2231)
        logging.info("Spatial reference set to NAD 1983 StatePlane Colorado North")
        logging.debug("Exiting set_spatial_reference()")
    except Exception as e:
        logging.error(f"Error in set_spatial_reference: {e}")


def apply_simple_renderer(layer_name):
    """
        Applies a simple red polygon renderer with black outline and 50% transparency.

        Args:
            layer_name (str): The name of the layer to style.
    """
    try:
        logging.debug("Entering apply_simple_renderer()")
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        map_doc = aprx.listMaps()[0]
        layer = map_doc.listLayers(layer_name)[0]
        sym = layer.symbology
        sym.renderer.symbol.color = {'RGB': [255, 0, 0, 100]}
        sym.renderer.symbol.outlineColor = {'RGB': [0, 0, 0, 100]}
        layer.symbology = sym
        layer.transparency = 50
        logging.info(f"Simple renderer applied to {layer_name}")
        logging.debug("Exiting apply_simple_renderer()")
    except Exception as e:
        logging.error(f"Error in apply_simple_renderer: {e}")


def apply_definition_query(layer_name):
    """
        Applies a definition query to show only addresses with Join_Count = 1.

        Args:
            layer_name (str): The name of the layer to filter.
    """
    try:
        logging.debug("Entering apply_definition_query()")
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        map_doc = aprx.listMaps()[0]
        layer = map_doc.listLayers(layer_name)[0]
        layer.definitionQuery = "Join_Count = 1"
        logging.info(f"Definition query applied to {layer_name}")
        logging.debug("Exiting apply_definition_query()")
    except Exception as e:
        logging.error(f"Error in apply_definition_query: {e}")


def exportMap(config):
    """
        Exports the map layout as a PDF, customizing the title with a user-provided subtitle.

        Args:
            config (dict): Configuration dictionary with project directory path.
    """
    try:
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
    except Exception as e:
        logging.error(f"Error in exportMap: {e}")


def main():
    """
        Main driver function that runs the entire workflow including:
        - ETL process
        - Buffering
        - Intersect and erase
        - Spatial join
        - Renderer, definition query, export to CSV and map layout.
    """
    try:
        config = load_config()

        logging.basicConfig(
            filename=f"{config.get('proj_dir')}wnv.log",
            filemode="w",
            level=logging.DEBUG
        )

        logging.info("Starting West Nile Virus Simulation")

        arcpy.env.workspace = f"{config.get('proj_dir')}WestNileOutbreak.gdb"
        arcpy.env.overwriteOutput = True

        set_spatial_reference()

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

        intersect_output = "final_analysis"
        intersect_buffers(buffer_outputs, intersect_output)

        sprayed_area = "Spray_Eligible_Area"
        erase_avoid_areas(intersect_output, avoid_buffer, sprayed_area)

        address_fc = "Boulder_addresses"
        joined_output = "Target_Addresses"
        spatial_join(address_fc, sprayed_area, joined_output)

        apply_simple_renderer("final_analysis")
        apply_definition_query("Target_Addresses")

        count_at_risk(joined_output)

        export_addresses_to_csv(joined_output, f"{config.get('proj_dir')}target_addresses.csv")

        exportMap(config)

    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
