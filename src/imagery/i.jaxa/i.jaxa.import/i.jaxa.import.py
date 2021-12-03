#!/usr/bin/env python

############################################################################
#
# MODULE:      i.jaxa.import
# AUTHOR(S):   Stefan Blumentrath
# PURPOSE:     Import Satellite Data Products downloaded from the JAXA data portal
# COPYRIGHT:   (C) 2021 by Stefan Blumentrath, and the GRASS development team
#
#              This program is free software under the GNU General Public
#              License (>=v2). Read the file COPYING that comes with GRASS
#              for details.
#
#############################################################################

#%module
#% description: Import Satellite Data Products downloaded from the JAXA data portal
#% keyword: imagery
#% keyword: satellite
#% keyword: JAXA
#% keyword: GCOM
#% keyword: G-portal
#% keyword: import
#%end

#%option G_OPT_M_DIR
#% key: input
#% description: Name for directory JAXA data is stored
#% required: yes
#% guisection: Input
#%end

#%option
#% key: prefix
#% description: Prefix for imported maps
#% required: yes
#% guisection: Input
#%end

#%option G_OPT_M_File
#% key: register_file
#% description: File to be used in t.register
#% required: no
#% guisection: Output
#%end

#%option
#% key: predefined_product
#% type: string
#% description: Pre-defined JAXA products to import
#% required: no
#% options: GCOM_C_LST_L2_250m_daily, GCOM_C_EVI_L3_1_24_deg_daily
#% guisection: Input
#%end

#%option
#% key: spacecraft
#% type: string
#% description: Datasets of which spacecraft to import
#% required: no
#% guisection: Product
#%end

#%option
#% key: sensor
#% type: string
#% description: Datasets of which sensor to search for
#% required: no
#% guisection: Product
#%end

#%option
#% key: product
#% type: string
#% description: Data product to import
#% required: no
#% guisection: Product
#%end

#%option
#% key: version
#% type: string
#% description: Version of the data product to import
#% required: no
#% options: 1, 2, 3
#% answer: 2
#% guisection: Product
#%end


#%option
#% key: version
#% type: string
#% description: Version of the data product to import
#% required: no
#% options: 1, 2, 3
#% answer: 2
#% guisection: Product
#%end

#%option
#% key: time_unit
#% type: string
#% description: Time unit of the dataset to import
#% required: no
#% options: 1_day,  8_days, 1_month
#% guisection: Product
#%end

#%option
#% key: projection
#% type: string
#% description: Projection of the dataset to import
#% required: no
#% options: EQA_1_dimension, EQA, EQR, PS_N, PS_S, Tile
#% guisection: Product
#%end

#%option
#% key: resolution
#% type: string
#% description: Resolution of the dataset to import
#% required: no
#% options: 250m, 1000m, 1_24deg, 1_12deg
#% guisection: Product
#%end

#%option
#% key: tiles
#% type: string
#% description: Tiles to import (ignored when not relevant)
#% required: no
#% guisection: Product
#%end

#%option
#% key: pattern
#% type: string
#% description: Regular expression used for filtering files to import
#% required: no
#% guisection: Filter
#%end

#%option
#% key: start
#% type: string
#% description: Start date ('YYYY-MM-DD')
#% guisection: Filter
#%end

#%option
#% key: end
#% type: string
#% description: End date ('YYYY-MM-DD')
#% guisection: Filter
#%end

#%option
#% key: nprocs
#% description: Sort order (see sort parameter)
#% type: integer
#% answer: 1
#% guisection: Optional
#%end

#%flag
#% key: l
#% description: Link data with r.external
#% guisection: Optional
#%end

#%flag
#% key: o
#% description: Override projection check (treat location as WGS84)
#% guisection: Optional
#%end

from datetime import datetime
from multiprocessing import Pool
import os
from osgeo import gdal, osr
from pathlib import Path
import re
import sys

# Lazy imports
import numpy as np

import h5py

import grass.script as gscript
from grass.script.utils import decode
from grass.pygrass.modules import Module, MultiModule, ParallelModuleQueue
from grass.temporal.datetime_math import (
    datetime_to_grass_datetime_string as grass_timestamp,
)


path = gscript.utils.get_lib_path(modname="jaxalib", libname="config")
if path is None:
    gscript.fatal("Not able to find the jaxalib library directory.")
sys.path.append(path)

import config as jl

# Move shared objects (e.g. product descriptions) into a jaxalib
# List content in the folder (datasets to be imported)

# have parallel import verion
# Read product metadata

# Test import like Sentinel-3 (r.in.xyz) vs GDAL based import with many or corner GCPs


# https://gportal.jaxa.jp/gpr/assets/mng_upload/GCOM-C/SGLI_Higher_Level_Product_Format_Description_en.pdf
# https://gportal.jaxa.jp/gpr/assets/mng_upload/GCOM-C/GCOM-C_SHIKISAI_Data_Users_Handbook_en.pdf
# https://www.thepythoncode.com/article/list-files-and-directories-in-ftp-server-in-python
# I've just tried with an FTP site and ogrinfo -ro -al -so
# /vsizip/vsicurl/ftp://user:password@example.com/foldername/file.zip/example.shp
# works
# /vsicurl/ is slow probably due to authentication
# gdalinfo "/vsicurl/ftp://ninsbl:anonymous@ftp.gportal.jaxa.jp/standard/GCOM-C/GCOM-C.SGLI/L3.LAND.EVI_/2/2018/09/GC1SG1_20180902D01D_X0000_3BSG_EVI_F_2000.h5"


# h5py
# "ftp://ftp.gportal.jaxa.jp/standard/GCOM-C/GCOM-C.SGLI/L2.LAND.LST_/2/2019/07/07/GC1SG1_20190707A01D_T0318_L2SG_LST_Q_2000.h5"
# import h5py
# input_file = "ftp://ninsbl:anonymous@ftp.gportal.jaxa.jp/standard/GCOM-C/GCOM-C.SGLI/L2.LAND.LST_/2/2019/07/07/GC1SG1_20190707A01D_T0318_L2SG_LST_Q_2000.h5"
# hdf_file = h5py.File(input_file, 'r')
# hdf_file['Image_data'].keys()
# band_name = 'LST'
# input_file_name=str(hdf_file['Global_attributes'].attrs['Product_file_name'][0][2:45])

# options = {"print": True, "output": "./", "nprocs": 2, "product_type": "standard", "spacecraft": 'GCOM-C', "sensor": 'GCOM-C.SGLI', "product": 'L2.LAND.LST_', "version": "2", "start_time": None, "end_time": None}
# flags = {"l": True}


import_modules = {
    "r.import": Module("r.import", run_=False),  # Not used (reproject in GDAL)
    "r.external": Module("r.external", run_=False),  # If not Tile projection and link
    "r.in.gdal": Module("r.in.gdal", run_=False),  # If not Tile projection
    "r.in.xyz": Module("r.in.xyz", run_=False),  # If Tile projection
}


def get_content(user_options, regex=re.compile(".*"), temporal_filter="modification"):
    """Filter files in input directory using a file name pattern and a temporal filer"""
    if regex is None:
        regex = re.compile(".*")
    content = Path(user_options["input"]).glob("*.h5")
    content = [
        file
        for file in content
        if regex["area_global" if str(file.name)[19] == "_" else "scene"].match(
            str(file.name)
        )
    ]
    if temporal_filter == "modification":
        content = [
            file
            for file in content
            if user_options["start"]
            <= datetime.utcfromtimestamp(os.path.getmtime(file))
            <= user_options["end"]
        ]
    elif temporal_filter == "sensing_time":
        # Extraction of sensing time needs to be checked
        content = [
            file
            for file in content
            if user_options["start"]
            <= datetime.strptime(str(file.name)[7:15], "%Y%m%d")
            <= user_options["end"]
        ]
    return content


def get_metadata(hdf_file):
    """Parse and transform metadata"""
    geo_metadata = {}
    global_attributes = hdf_file["Global_attributes"]
    subdatasets = hdf_file
    bands = sorted(hdf_file["Image_data"].keys())
    global_attributes = hdf_file["/Global_attributes"].attrs
    grass_metadata = {
        "title": None,
        # [history=phrase]
        "units": None,
        "vdatum": None,
        "source1": decode(global_attributes["Algorithm_developer"][0]),
        "source2": "{}, {}".format(
            decode(global_attributes["Satellite"][0]),
            decode(global_attributes["Satellite"][0]),
        ),
        "description": "{}; {}, version {}".format(
            decode(global_attributes["Dataset_description"][0]),
            decode(global_attributes["Product_name"][0]),
            decode(global_attributes["Algorithm_version"][0]),
        ),
    }
    temporal_metadata = {
        "start_time": datetime.strptime(
            decode(global_attributes["Image_start_time"][0]), "%Y%m%d %H:%M:%S.%f"
        ),
        "end_time": datetime.strptime(
            decode(global_attributes["Image_end_time"][0]), "%Y%m%d %H:%M:%S.%f"
        ),
    }
    """[('Algorithm_developer', array([b'Japan Aerospace Exploration Agency (JAXA)'], dtype='|S41')),
     ('Algorithm_version', array([b'2.00'], dtype='|S4')),
     ('Dataset_description', array([b'Level-3 daily spatial binning file of NDVI'], dtype='|S42')),
     ('Image_end_time', array([b'20190701 22:45:10.800'], dtype='|S21')),
     ('Image_start_time', array([b'20190701 00:00:00.000'], dtype='|S21')),
     ('Parameter_version', array([b'000.01'], dtype='|S6')),
     ('Product_file_name', array([b'GC1SG1_20190701A01D_D0000_3MSG_NDVIF_2000.h5'], dtype='|S44')),
     ('Product_level', array([b'Level-3'], dtype='|S7')),
     ('Product_name', array([b'Level-3 binned map product'], dtype='|S26')),
     ('Product_version', array([b'2000'], dtype='|S4')),
     ('Satellite', array([b'Global Change Observation Mission - Climate (GCOM-C)'], dtype='|S52')),
     ('Sensor', array([b'Second-generation Global Imager (SGLI)'], dtype='|S38'))]
      """
    geo_attributes = hdf_file["Geometry_data"].attrs
    geo_metadata["projection"] = decode(geo_attributes["Image_projection"][0])
    image_metadata = {}
    for a in hdf_file["/Image_data"]:
        image_attrs = hdf_file["/Image_data"][a].attrs
        image_metadata[a] = {}
        for key, attr in image_attrs.items():
            image_metadata[a][key] = (
                decode(attr[0]) if str(attr.dtype).startswith("|S") else attr
            )
    return grass_metadata, temporal_metadata, geo_metadata, image_metadata

    """
'Data_description': 'QA information flag ([bit field 1-0]: water(land frac.=0%)=00, mostly water(0<land frac.<50%)=01, mostly coastal(50<land frac.<100%)=10, land(land frac=100%)=11. If no input data available, all bit fields are set to 1 (i.e., flag value = 255). If pixel value in l2 data is Error_DN, flag value = 254)',
'Error_DN': array([255], dtype=uint8),
'Maximum_valid_DN': array([253],
dtype=uint16),
'Minimum_valid_DN': array([0], dtype=uint16),
'Offset': array([0.], dtype=float32),
'Slope': array([1.], dtype=float32),
'Unit': 'NA'
    """


def check_projection(user_flags):
    source_crs = osr.SpatialReference()
    source_crs.ImportFromEPSG(4326)
    target_crs = osr.SpatialReference(
        wkt=gscript.read_command("g.proj", flags="w").rstrip()
    )
    if (
        target_crs.ExportToPrettyWkt() == source_crs.ExportToPrettyWkt()
        or user_flags["o"]
    ):
        target_crs = None
    return target_crs


def mask_and_unscale(image):
    """"""
    required_keys = {
        "Error_DN",
        "Maximum_valid_DN",
        "Minimum_valid_DN",
        "Offset",
        "Slope",
    }
    if not required_keys.intersection(set(image.attrs.keys())) == required_keys:
        gscript.fatal("Fatal, cannot mask and unscale image")
    img = np.ma.masked_equal(image[:], image.attrs["Error_DN"])
    img = image.attrs["Offset"][0] + img * image.attrs["Slope"][0]
    img = np.ma.masked_inside(
        img, image.attrs["Minimum_valid_DN"][0], image.attrs["Maximum_valid_DN"][0]
    )
    return img


def compute_tile_gcps(tile):
    """Compute lat lon coordinates for GCPs
    for a given tile
    tile is an open HDF5 dataset
    Likely needed for EQA projection
    """

    global_attributes = tile["/Global_attributes"].attrs
    geo_attributes = tile["Geometry_data"].attrs
    image_attributes = tile["/Image_data"].attrs

    if "Tile_number" in global_attributes:
        tile_number = decode(global_attributes["Tile_number"][0])
    else:
        product_file_name = decode(global_attributes["Product_file_name"][0])
        tile_number = product_file_name.split("_")[2][1:]

    # Get tile numbers
    vtile = int(tile_number[0:2])  # Tile number in vertical direction
    htile = int(tile_number[2:])  # Tile number in horizontal direction

    # Get image resolution
    if 0.002 < image_attributes["Grid_interval"][0] < 0.003:
        img_resolution = 250.0
    elif 0.0083 < image_attributes["Grid_interval"][0] < 0.0084:
        img_resolution = 1000.0

    # Get geometry information
    # Number of pixels in vertical direction in a tile (corresponds to resolution)
    lin_tile = image_attributes["Number_of_lines"][0]
    # Number of pixels in horizontal direction in a tile (corresponds to resolution)
    col_tile = image_attributes["Number_of_pixels"][0]

    lin_num = np.tile(np.arange(0, lin_tile).reshape(-1, 1), (1, col_tile))
    col_num = np.tile(np.arange(0, col_tile), (lin_tile, 1))
    latlons = []

    vtile_n = 18  # Total number of tiles in vertical direction
    htile_n = 36  # Total number of tiles in horizontal direction

    d = 180.0 / lin_tile / vtile_n

    nl = 180.0 / d
    np_0 = 2 * float(Decimal(180.0 / d).quantize(Decimal("0"), rounding=ROUND_HALF_UP))

    lin_total = lin_num + (vtile * lin_tile)
    col_total = col_num + (htile * col_tile)

    lat = 90.0 - (lin_total + 0.5) * d
    np_i = (np_0 * np.cos(np.deg2rad(lat)) + 0.5).astype(np.int32).astype(np.float32)
    lon = 360.0 / np_i * (col_total - np_0 / 2.0 + 0.5)

    return [pixels, latlons]


def create_l3_vrt(file, link, target_projection):
    """Create a vrt linking to L3 (bined) products (Equal Lat/Lon Coordinate (EQR)"""
    vrts = []
    # Define path to output dataset
    # Scaling has to be done outside GDAL
    # Use proj window from curent region
    in_ds = gdal.Open(str(file))
    for sds in in_ds.GetSubDatasets():
        sds_name = sds[0].split("/")[-1]
        vrt_path = file.parent.joinpath(
            f"{file.with_suffix('').name}_{sds_name}_wgs84.vrt"
        )
        out_vrt = gdal.Translate(
            str(vrt_path),
            str(sds[0]),
            outputSRS="EPSG:4326",
            outputBounds=[-180, 90, 180, -90],
            noData=65535,
            format="VRT",
            # scale=0.02,
            unscale="YES",
        )
        out_vrt = None

        if target_projection is not None and link:
            vrt_path = file.parent.joinpath(
                f"{file.with_suffix('').name}_{sds_name}.vrt"
            )
            out_vrt = gdal.Warp(
                vrt_path,
                str(sds[0]),
                srcSRS="EPSG:4326",
                dstSRS=target_projection,
                format="VRT",
                outputBounds=[-180, 90, 180, -90],
                outputBoundsSRS="EPSG:4326",
            )
            out_vrt = None
        # gcps = compute_l2_gcps(tile)
        # gdal_translate -of Gtiff a_srs EPSG:4326 a_ullr -180 90 180 -90
        vrts.append((sds_name, vrt_path))
    # Close GDAL dataset
    in_ds = None
    return vrts


def create_l2_vrt(tile, level):
    """Create a vrt linking to L2 (non-tiled) products"""
    in_ds = gdal.Open()
    if level == 2:
        gcps = compute_l2_gcps(tile)
        out_vrt = gdal.Warp(gcps)
    else:
        # gdal_translate -of Gtiff a_srs EPSG:4326 a_ullr -180 90 180 -90
        out_vrt = gdal.Warp(gcp)
    out_vrt = None


def import_module_queue(import_module, color_table):
    """Import a dataset using standard GRASS GIS modules"""
    queue = ParallelModuleQueue()
    # "r.external" or "r.in.gdal"
    import_mod = Module(import_module, run_=False)
    meta_mod = Module("r.support", **grass_metadata, run_=False)
    time_mod = Module("r.timestamp", run_=False)
    color_mod = Module("r.colors", clor_table, run_=False)
    queue.put(MultiModule([import_mot, meta_mod, time_mod, color_mod]))
    queue.wait()
    return 0


def import_xyz():
    """Import a dataset using np arrays and r.in.xyz"""
    return 0


def run_import(path, source_projection, target_projection):
    """ """

    hdf_file = h5py.File(str(path), "r")
    grass_metadata, temporal_metadata, geo_metadata, image_metadata = get_metadata(
        hdf_file
    )
    hdf_file.close()

    print(grass_metadata, temporal_metadata, geo_metadata, image_metadata)

    if source_projection in ["EQR", "EQA"]:
        # Import routine for L3 products
        vrts = create_l3_vrt(path, True, target_projection)  # , scale)
        if flags["l"]:
            import_mod = "r.external"
        else:
            import_mod = "r.in.gdal"
    else:
        xyz_file = compute_tile_gcps(tile)
        import_module = Module("r.in.xyz", run_=False)

    # Setup modules
    meta_mod = Module("r.support", run_=False)
    time_mod = Module("r.timestamp", run_=False)
    # color_mod = Module("r.colors", color_table, run_=False)

    print(vrts)
    for sds in vrts:
        meta = grass_metadata.copy()
        mapname = sds[1].with_suffix("").name.replace("_wgs84", "")
        meta["title"] = image_metadata[sds[0]]["Data_description"]
        meta["units"] = image_metadata[sds[0]]["Unit"]
        # , temporal_metadata, geo_metadata, image_metadata
        Module(import_mod, input=str(sds[1]), output=mapname)
        Module("r.support", map=mapname, **meta)
        Module(
            "r.timestamp",
            map=mapname,
            date="{}/{}".format(
                grass_timestamp(temporal_metadata["start_time"]),
                grass_timestamp(temporal_metadata["end_time"]),
            ),
        )

    if not source_projection in ["EQR", "EQA"]:
        os.remove(xyz_file)
    return 0


def print_register_file(register_file, register_text):
    """ """
    if register_file is None:
        print(register_text)
    else:
        register_file.write_text()
    return 0


def main():
    """Do the main work"""
    options["product_type"] = "standard"
    options["nprocs"] = int(options["nprocs"])

    # Get current projection
    target_crs = osr.SpatialReference(
        wkt=gscript.read_command("g.proj", flags="w").rstrip()
    )

    # Temporal filter
    date_msg = "Cannot parse given {} date. Please check the input format."
    for date_ref in ["start", "end"]:
        if options[date_ref]:
            try:
                options[date_ref] = datetime.strptime(options[date_ref], "%Y-%m-%d")
            except Exception:
                gscript.fatal(_(date_msg.format(date_ref)))
    if options["start"] > options["end"]:
        gscript.fatal(_("start cannot be after end"))

    if options["tiles"]:
        # Tile numbers are optional
        tile_numbers = options["tiles"].split(",")  # ["0317", "0318", "0319"]
        # Check provided tile numbers
        for tn in tile_numbers:
            if not tn.isdigit() or len(tn) != 4:
                gscript.fatal(
                    _(
                        "Invalid tile definition for requested tile {}"
                        "Only tile numbers between 0000 and 1735 are valid.".format(tn)
                    )
                )
            if not 0 <= int(tn[0:2]) <= 17:
                gscript.fatal(
                    _(
                        "Invalid tile definition for requested tile {}"
                        "In vertical direction (first two digits), "
                        "only numbers between 00 and 17 are valid.".format(tn)
                    )
                )
            if not 0 <= int(tn[2:4]) <= 35:
                gscript.fatal(
                    _(
                        "Invalid tile definition for requested tile {}"
                        "In horizontal direction (last two digits), "
                        "only numbers between 00 and 35 are valid.".format(tn)
                    )
                )
    else:
        tile_numbers = None

    # Get product definition
    product_definition = jl.get_product_definition(options)
    regex = jl.compile_regex(product_definition, tiles=tile_numbers)
    for key, val in product_definition.items():
        options[key] = val

    # List relevant content
    content = get_content(options, regex=regex, temporal_filter="sensing_time")

    # Ckeck if projections match
    target_projection = check_projection(flags)

    # Get metadata and transform if needed / requested

    # Identify import method (r.import/r.in.gdal/r.external)

    # Run import
    if options["nprocs"] > 1:
        pool = Pool(options["nprocs"])
        result = pool.starmap(
            run_import,
            [
                (file, product_definition["projection"], target_projection)
                for file in content
            ],
        )
        # get_content, [(user, base_url, time_slot, user_options, regex) for time_slot in time_slots])
    else:
        result = []
        for file in content:
            result.append(create_l3_vrt(file, True, target_projection))


if __name__ == "__main__":
    options, flags = gscript.parser()

    # lazy import
    try:
        import h5py
    except ImportError:
        gscript.fatal(_("Cannot import h5py library. Please install it first."))

    main()


# https://shikisai.jaxa.jp/faq/docs/geotiff_conversion_20201106A_EN.pdf


"""
Conversion of L3 NDVI (Normalized Difference Vegetation Index) (Equal Lat/Lon
Coordinate (EQR))
ï¼ï¼Get the dataset name by GDALINFO
Driver: HDF5/Hierarchical Data Format Release 5
Files: GC1SG1_20200401D01M_D0000_3MSG_NDVIF_1001.h5
Size is 512, 512
ã»
ã»
Subdatasets:
SUBDATASET_1_NAME=HDF5:"GC1SG1_20200401D01M_D0000_3MSG_NDVIF_1001.h5"://Image_data/NDVI_AVE
SUBDATASET_1_DESC=[4320x8640] //Image_data/NDVI_AVE (16-bit unsigned integer)
SUBDATASET_2_NAME=HDF5:"GC1SG1_20200401D01M_D0000_3MSG_NDVIF_1001.h5"://Image_data/NDVI_QA_flag
SUBDATASET_2_DESC=[4320x8640] //Image_data/NDVI_QA_flag (8-bit unsigned character)
gdalinfo GC1SG1_20200401D01M_D0000_3MSG_NDVIF_1001.h5
ï¼ï¼Convert to GeoTIFF format by GDAL_TRANSLATE
By the following command, select the target dataset file, and convert to GeoTIFF format.
gdal_translate -of Gtiff a_srs EPSG:4326 a_ullr -180 90 180 -90
HDF5:"GC1SG1_20200401D01M_D0000_3MSG_NDVIF_1001.h5"://Image_data/NDVI_AVE NDVI_output.tif
Projection code
of image file
Input dataset file name
The image file name
The range of input file (upper
left (x, y), lower right (x, y)
Output file name



Conversion of L2 EVI (Enhanced Vegetation Index) (Equal Area Coordinate (EQA))
ï¼ï¼ Get the dataset name by GDALINFO
Subdatasets:
SUBDATASET_1_NAME=HDF5:"GC1SG1_20200701D01M_T0428_L2SG_EVI_Q_2000.h5"://Image_data/EVI_AVE
SUBDATASET_1_DESC=[4800x4800] //Image_data/EVI_AVE (16-bit unsigned integer)
gdalinfo GC1SG1_20200701D01M_T0428_L2SG_EVI_Q_2000.h5
ï¼ï¼ Convert to GeoTIFF format by GDAL_TRANSLATE
gdal_translate -of Gtiff -gcp 0 0 155.5707804 49.99895833 -gcp 4799 0 171.1244553 49.99895833 -gcp
4799 4799 143.5961321 40.00104167 -gcp 0 4799 130.5445343 40.00104167 -gcp 2400 0 163.3492384
49.99895833 -gcp 4799 2400 155.5595384 44.99895833 -gcp 2400 4799 137.071693 40.00104167 -gcp 0
2400 141.4205745 44.99895833 -gcp 2400 2400 148.4915296 44.99895833
HDF5:"GC1SG1_20200701D01M_T0428_L2SG_EVI_Q_2000.h5"://Image_data/EVI_AVE output.tif
Input dataset file name Output file name
< example viewing by QGIS >
How to convert to GeoTIFF format from the Shikisai L2 tile products.
A
Tile Number
ï¼ï¼Calculate GCP points for tile image
Calculate the GCP points according to the resolution of the target tile image from the formula in " 4.1.4.1 Level2 Product
Generation Unit " of the GCOM-C âSHIKISAIâ Data Users Handbook.
The position of the GCP points on
the image (resolution : Q(250m))
â Set the latitude and longitude
of 9 points as GCP points.
Reproject to EPSG:4326
gdalwarp -of Gtiff t_srs EPSG:4326 output.tif output2.tif
Input file name Output file name


y. The latitude/longitude to each pixel of the tile can be calculated as
below:
For the tile whose latitude/longitude (ÝÜ½ÝÝ,Ý (Ýis to be calculated, the pixel number of the pixel
in vertical direction (top => bottom) is defined as ÝÝÝ, and that in horizontal direction (left =>
right) is defined as Ü¿Ý .ÝThe pixel number of the upper left pixel within a tile is defined as (0,0).
e.g.) Case of resolution 250 m [Image size 4800 x 4800 pixel, tile number v05h29]
ã»ÝÝÝà¯§à¯à¯à¯ = 4800 : Number of pixels in vertical direction in a tile (corresponds to 250 m)
ã»Ü¿ÝÝà¯§à¯à¯à¯ = 4800 : Number of pixels in horizontal direction in a tile (corresponds to 250 m)
ã»ÝÝ = ÝÝÝ5 : Tile number in vertical direction
ã»âÝ = ÝÝÝ29 : Tile number in horizontal direction
ã»ÝÝà¯¡ÝÝÝà¯¨à¯  = 18 : Total number of tiles in vertical direction
ã»âÝà¯¡ÝÝÝà¯¨à¯  = 36 : Total number of tiles in horizontal direction
ã»Ü°Ü® : Total number of pixels from south pole to north pole
ã»Ü°Ü²à¬´ : Total number of pixels in east-west direction at the equator
Ý = 180.0/ÝÝÝà¯§à¯à¯à¯/ÝÝà¯¡ÝÝÝà¯¨à¯  [ÝÝÝ/ÝÝÝ [ÝÝ
Ü°Ü® = 180.0/Ý ï¼from S-pole to N-poleï¼
Ü°Ü²à¬´ = 2 Ã Ü°Ü«]Ü¶Ü°180.0/Ý] ï¼from 180W to 180Eï¼
ÝÝÝà¯§à¯¢à¯§à¯à¯ = ÝÝÝ + áºÝÝÝÝÝ Ã ÝÝÝà¯§à¯à¯à¯á»
Ü¿ÝÝà¯§à¯¢à¯§à¯à¯ = Ü¿Ý + ÝáºâÝÜ¿ Ã ÝÝÝÝÝà¯§à¯à¯à¯á»
The latitude/longitude (ÝÜ½ÝÝ,Ý (Ýare as follows:
ÝÜ½Ý = 90.0 â áºÝÝÝà¯§à¯¢à¯§à¯à¯ + 0.5á» Ã Ý
[á»ÝÜ½ÝáºÝÝÜ¿ Ã à¬´Ü²Ü°]Ü¶Ü°Ü«Ü° = à¯Ü²Ü°
ÝÝ = Ý360.0/Ü°Ü²à¯ Ã áºÜ¿ÝÝà¯§à¯¢à¯§à¯à¯ â Ü°Ü²à¬´/2 + 0.5
"""


"""
import numpy as np
import logging
from decimal import Decimal, ROUND_HALF_UP
from abc import ABC, abstractmethod, abstractproperty
from spot.utility import bilin_2d
from spot.config import PROJ_TYPE

# =============================
#  Level-2 template class
# =============================
class L2Interface(ABC):
    @property
    @abstractmethod
    def PROJECTION_TYPE(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def ALLOW_PROJECTION_TYPE(self):
        return NotImplementedError()

    def __init__(self, h5_file, product_id):
        self.h5_file = h5_file
        self.product_id = product_id

        geo_data_grp_attrs = self.h5_file['Geometry_data'].attrs
        self.geo_n_pix = geo_data_grp_attrs['Number_of_pixels'][0]
        self.geo_n_lin = geo_data_grp_attrs['Number_of_lines'][0]

        img_data_grp_attrs = self.h5_file['Image_data'].attrs
        self.img_n_pix = img_data_grp_attrs['Number_of_pixels'][0]
        self.img_n_lin = img_data_grp_attrs['Number_of_lines'][0]

    def get_product_data(self, prod_name:str):
        dset = self.h5_file['Image_data/' + prod_name]

        # Return uint16 type data if the product is QA_flag or Line_tai93
        if 'QA_flag' == prod_name or 'Line_tai93' == prod_name:
            return dset[:]

        # Validate
        data = dset[:].astype(np.float32)
        if 'Error_DN' in dset.attrs:
            data[data == dset.attrs['Error_DN'][0]] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
            if 'Maximum_valid_DN' in dset.attrs:
                data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            if 'Minimum_valid_DN' in dset.attrs:
                data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        # Convert DN to physical value
        data = data * dset.attrs['Slope'][0] + dset.attrs['Offset'][0]

        return data

    def get_geometry_data(self, data_name:str, **kwargs):
        interval = kwargs['interval']

        dset = self.h5_file['Geometry_data/' + data_name]
        data = dset[:]
        if 'Latitude' is not data_name and 'Longitude' is not data_name:
            data = dset[:].astype(np.float32)
            if 'Error_DN' in dset.attrs:
                data[data == dset.attrs['Error_DN'][0]] = np.NaN
            with np.warnings.catch_warnings():
                np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
                if 'Maximum_valid_DN' in dset.attrs:
                    data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
                if 'Minimum_valid_DN' in dset.attrs:
                    data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

            data = data * dset.attrs['Slope'][0] + dset.attrs['Offset'][0]

        # Finish if interval is none
        if interval is None or interval == 'none':
            return data

        # Interpolate raw data
        if interval == 'auto':
            interp_interval = dset.attrs['Resampling_interval'][0]
        else:
            interp_interval = interval

        lon_mode = False
        if 'Longitude' == data_name:
            lon_mode = True
        if interp_interval > 1:
            data = bilin_2d(data, interp_interval, lon_mode)

        # Trim away the excess pixel/line
        (data_size_lin, data_size_pxl) = data.shape
        if (kwargs['fit_img_size'] is True) and (self.img_n_lin <= data_size_lin) and (self.img_n_pix <= data_size_pxl):
            data = data[:self.img_n_lin, :self.img_n_pix]

        return data

    @abstractmethod
    def get_geometry_data_list(self):
        raise NotImplementedError()

    def get_product_data_list(self):
        return list(self.h5_file['/Image_data'].keys())

    def get_unit(self, prod_name: str):
        # Get attrs set
        unit_name = 'Unit'
        if prod_name in self.get_product_data_list():
            grp_name = '/Image_data/'
        else:
            grp_name = '/Geometry_data/'
        attrs = self.h5_file[grp_name + prod_name].attrs

        # Get unit
        if unit_name not in attrs:
            return 'NA'
        return attrs[unit_name][0].decode('UTF-8')

# =============================
# Level-2 map-projection class
# =============================


class Tile(L2Interface):
    PROJECTION_TYPE = PROJ_TYPE.TILE.name
    ALLOW_PROJECTION_TYPE = [PROJECTION_TYPE, PROJ_TYPE.EQR.name]

    def __init__(self, h5_file, product_id):
        super().__init__(h5_file, product_id)

        glb_attrs = h5_file['/Global_attributes'].attrs
        if 'Tile_number' in glb_attrs:
            tile_numbner = glb_attrs['Tile_number'][0].decode('UTF-8')
            self.vtile = int(tile_numbner[0:2])
            self.htile = int(tile_numbner[2:])
        else:
            product_file_name = glb_attrs['Product_file_name'][0].decode('UTF-8')
            tile_numbner = product_file_name.split('_')[2]
            self.vtile = int(tile_numbner[1:3])
            self.htile = int(tile_numbner[3:])

        self.img_spatial_reso = h5_file['/Image_data'].attrs['Grid_interval'][0]
        if self.img_spatial_reso > 0.0083 and self.img_spatial_reso < 0.0084:
            self.img_spatial_reso = 1000.
        elif self.img_spatial_reso > 0.002 and self.img_spatial_reso < 0.003:
            self.img_spatial_reso = 250.

    def get_geometry_data(self, data_name:str, **kwargs):

        if data_name == 'Latitude' or data_name == 'Longitude':
            # This algorithm referred to 'GCOM-C Data Users Handbook (SGC-180024)'
            lin_num = np.tile(np.arange(0, self.img_n_lin).reshape(-1,1), (1, self.img_n_pix))
            col_num = np.tile(np.arange(0, self.img_n_pix), (self.img_n_lin, 1))

            d = 180. / self.img_n_lin / 18
            np_0 = 2 * float(Decimal(180. / d).quantize(Decimal('0'), rounding=ROUND_HALF_UP))
            lin_total = lin_num + (self.vtile * self.img_n_lin)
            col_total = col_num + (self.htile * self.img_n_pix)

            lat = 90. - (lin_total + 0.5) * d
            if data_name == 'Latitude':
                return lat
            elif data_name == 'Longitude':
                np_i = (np_0 * np.cos(np.deg2rad(lat)) + 0.5).astype(np.int32).astype(np.float32)
                lon = 360. / np_i * (col_total - np_0 / 2. + 0.5)

                if self.htile > 17:
                    lon[lon > 180] = np.NaN
                else:
                    lon[lon < -180] = np.NaN
                return lon
        else:
            return super().get_geometry_data(data_name, **kwargs)

        return None

    def get_geometry_data_list(self):
        return ['Latitude', 'Longitude'] + list(self.h5_file['/Geometry_data'].keys())

    def get_allow_projection_type(self):
        return self.ALLOW_PROJECTION_TYPE

    def get_unit(self, prod_name: str):
        if prod_name == 'Latitude' or prod_name == 'Longitude':
            return 'degree'

        return super().get_unit(prod_name)

class Scene(L2Interface):
    PROJECTION_TYPE = PROJ_TYPE.SCENE.name
    ALLOW_PROJECTION_TYPE = [PROJECTION_TYPE, PROJ_TYPE.EQR.name]

    def __init__(self, h5_file, product_id):
        super().__init__(h5_file, product_id)
        self.scene_number = h5_file['/Global_attributes'].attrs['Scene_number'][0]
        self.path_number = h5_file['/Global_attributes'].attrs['RSP_path_number'][0]

        img_data_grp_attrs = self.h5_file['Image_data'].attrs
        self.img_spatial_reso = img_data_grp_attrs['Grid_interval'][0]

    def get_geometry_data_list(self):
        return list(self.h5_file['/Geometry_data'].keys())

    def get_allow_projection_type(self):
        return self.ALLOW_PROJECTION_TYPE

# =============================
# Level-2 area class
# =============================


class RadianceL2(Tile):
    # -----------------------------
    # Public
    # -----------------------------
    def get_product_data(self, prod_name:str):
        if 'Land_water_flag' in prod_name:
            return self._get_land_water_flag()

        if 'Lt_' in prod_name:
            return self._get_Lt(prod_name)

        if 'Rt_' in prod_name:
            return self._get_Rt(prod_name)

        if 'Stray_light_correction_flag_' in prod_name:
            return self._get_stray_light_correction_flag(prod_name)

        return super().get_product_data(prod_name)


    def get_product_data_list(self):
        prod_list = super().get_product_data_list()
        for prod in prod_list:
            if ('Lt_P' in prod) or ('Lt_S' in prod) or ('Lt_V' in prod):
                prod_list.append(prod.replace('Lt', 'Rt'))
                prod_list.append(prod.replace('Lt', 'Stray_light_correction_flag'))

        prod_list = sorted(prod_list)
        return prod_list

    def get_unit(self, prod_name: str):
        if 'Rt_' in prod_name:
            return 'NA'

        return super().get_unit(prod_name)

    # -----------------------------
    # Private
    # -----------------------------
    def _get_land_water_flag(self):
        dset = self.h5_file['Image_data/Land_water_flag']
        data = dset[:].astype(np.float32)
        if 'Error_DN' in dset.attrs:
            data[data == dset.attrs['Error_value'][0]] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
            data[data > dset.attrs['Maximum_valid_value'][0]] = np.NaN
            data[data < dset.attrs['Minimum_valid_value'][0]] = np.NaN

        return data

    def _get_Lt(self, prod_name):
        dset = self.h5_file['Image_data/' + prod_name]
        dn_data = dset[:]
        mask = dset.attrs['Mask'][0]
        data = np.bitwise_and(dn_data, mask).astype(np.float32)
        data = data * dset.attrs['Slope'] + dset.attrs['Offset']
        data[dn_data == dset.attrs['Error_DN']] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
            data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        return data

    def _get_Rt(self, prod_name):
        prod_name = prod_name.replace('Rt_', 'Lt_')
        dset = self.h5_file['Image_data/' + prod_name]
        dn_data = dset[:]
        mask = dset.attrs['Mask'][0]
        data = np.bitwise_and(dn_data, mask).astype(np.float32)
        data = data * dset.attrs['Slope_reflectance'] + dset.attrs['Offset_reflectance']
        data[dn_data == dset.attrs['Error_DN']] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
            data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        solz_name = 'Solar_zenith'
        if 'Lt_P' in prod_name:
            solz_name = 'Solar_zenith_PL'
        cos_theta_0 = np.cos(np.deg2rad(self.get_geometry_data(solz_name, interval='auto', fit_img_size=True)))
        data = data / cos_theta_0

        return data

    def _get_stray_light_correction_flag(self, prod_name):
        prod_name = prod_name.replace('Stray_light_correction_flag_', 'Lt_')
        dset = self.h5_file['Image_data/' + prod_name]
        dn_data = dset[:]
        data = np.bitwise_and(dn_data, 0x8000)
        data[dn_data == dset.attrs['Error_DN']] = 0

        return data > 0

class OceanL2(Scene):
    def get_product_data(self, prod_name:str):
        if 'Rrs' not in prod_name:
            return super().get_product_data(prod_name)

        # Get Rrs data
        real_prod_name = prod_name.replace('Rrs', 'NWLR')
        dset = self.h5_file['Image_data/' + real_prod_name]

        # Validate
        data = dset[:].astype(np.float32)
        if 'Error_DN' in dset.attrs:
            data[data == dset.attrs['Error_DN'][0]] = np.NaN
        with np.warnings.catch_warnings():
            np.warnings.filterwarnings('ignore', r'invalid value encountered in (greater|less)')
            if 'Maximum_valid_DN' in dset.attrs:
                data[data > dset.attrs['Maximum_valid_DN'][0]] = np.NaN
            if 'Minimum_valid_DN' in dset.attrs:
                data[data < dset.attrs['Minimum_valid_DN'][0]] = np.NaN

        # Convert DN to physical value
        data = data * dset.attrs['Rrs_slope'][0] + dset.attrs['Rrs_offset'][0]

        return data

    def get_product_data_list(self):
        prod_list = super().get_product_data_list()
        if self.product_id == 'NWLR':
            prod_list = prod_list + ['Rrs_380', 'Rrs_412', 'Rrs_443', 'Rrs_490', 'Rrs_530', 'Rrs_565', 'Rrs_670']

        return prod_list

    def get_unit(self, prod_name: str):
        # Get attrs set
        unit_name = 'Unit'
        real_prod_name = prod_name
        if 'Rrs' in prod_name:
            real_prod_name = prod_name.replace('Rrs', 'NWLR')
            unit_name = 'Rrs_unit'
        attrs = self.h5_file['/Image_data/' + real_prod_name].attrs

        # Get unit
        if unit_name not in attrs:
            return 'NA'
        return attrs[unit_name][0].decode('UTF-8')


class LandL2(Tile):

    def get_product_data(self, prod_name: str):
        return super().get_product_data(prod_name)


class AtmosphereL2(Tile):

    def get_product_data(self, prod_name: str):
        return super().get_product_data(prod_name)


class CryosphereL2(Tile):

    def get_product_data(self, prod_name: str):
        return super().get_product_data(prod_name)


class CryosphereOkhotskL2(Scene):

    def get_product_data(self, prod_name: str):
        return super().get_product_data(prod_name)

    def get_flag(self, *args):
        return None

#
"""
