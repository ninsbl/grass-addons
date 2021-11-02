#!/usr/bin/env python3

import re

ftp_url = "ftp.gportal.jaxa.jp"

# Time unit is required input
time_units = {
    "1_day": "01D",
    "8_days": "08D",
    "1_month": "01M",
}

# Projection is required input
projections = {
    "EQA (1- dimension)": "X",
    "EQA": "A",
    "EQR": "D",
    "PS-N": "N",
    "PS-S": "S",
    "Tile": "T",
}

# Processing level is required input
processing_level = {
    "L2": "L2",
    "L3": "3M",
    # "L3Bin statistic": "3B",  # Probably not relevant as non spatial
    # "L3Map": "3M",
}

# Resolution is required input
resolutions = {
    "1000m": "K",
    "250m": "Q",
    "1_24deg": "F",
    "1_12deg": "C",
}

product_type = ["standard", "nrt"]  # Default: standard

spacecraft = ["GCOM-C", "GCOM-W"]  # Many more

sensor = ["GCOM-C.SGLI", "GCOM-W.AMSR2"]  # many many more

level = [2, 3]  # L1 is basically not supported

domain = ["LAND", "OCEAN", "CRYOSPHERE"]

# product = ["LST_", "EVI_"]

version = [1, 2, 3]

url_components = ["product_type", "spacecraft", "sensor", "product", "version"]

predefined_products = {
    "GCOM_C_LST_L2_250m_daily": {
        # Folder name components
        "product_type": "standard",
        "spacecraft": "GCOM-C",
        "sensor": "GCOM-C.SGLI",
        "product": "L2.LAND.LST_",  # Also in filename
        "version": 2,  # Also in filename
        "level": 2,  # Also in filename
        # File name specific components
        "resolution": "250m",
        "time_unit": "1_day",
        "projection": "Tile",
    },
    "GCOM_C_EVI_L3_1_24_deg_daily": {
        # Folder name components
        "product_type": "standard",
        "spacecraft": "GCOM-C",
        "sensor": "GCOM-C.SGLI",
        "product": "L3.LAND.EVI_",  # Also in filename
        "version": 2,  # Also in filename
        "level": 3,  # Also in filename
        # File name specific components
        "resolution": "1_24deg",
        "time_unit": "1_day",
        "projection": "EQR",
    },
}

"""
[
    (
        file[0],
        datetime.strptime(file[1]["modify"], "%Y%m%d%H%M%S"),
        round(float(file[1]["size"]) / 1024.0 / 1024.0, 2),
    )
    for file in files
    if "size" in file[1]
]
"""

def compile_regex(product_definition, tiles=None):
    """Filter files to download
        https://gportal.jaxa.jp/gpr/assets/mng_upload/GCOM-C/SGLI_Higher_Level_Product_Format_Description_en.pdf

    Table 3.2-1

    Table 3.3-1
    Table 3.3-2

    Table 3.6-1
    Table 3.6-2

    No. Processing level Unit for creating product Resolution
    1.L2 Scene 250 m/500 m
    2. 1 km
    3. Tile 250 m
    4. 1 km
    5. EQA 1/24 deg (4.6 km)
    6. L3 EQA、EQR 1/24deg (4.6km)
    7. 1/12deg (9.3km)
    8. PS 1/24deg (4.6km

        2 Level 2 (Scene Type) Granule IDs
        Byte    1 2 3 4 5 6 7 8 9 10 11 12 13 114 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41
        GID     G C 1 S G 1 _ Y Y Y Y M M D D H H m m s P P P S S _ L L x1 x2 _ K K K K r _ a p p p
        Example G C 1 S G 1 _ 2 0 1 1 1 1 1 3 2 3 4 5 6 0 1 2 0 6 _ L 2 S G _ N W L R K _ 1 0 0 1
        Example GC1SG1_201111132345601206_L2SG_NWLRK_1001

        Level 2 (Area, Global Type) and Level 3 Granule IDs
        Byte    1 2 3 4 5 6 7 8 9 10 11 12 13 114 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41
        GID     G C 1 S G 1 _ Y Y Y Y M M D D m t t t _ g A A A A _ L L x1 x2 _ K K K K r _ a p p p
        Example G C 1 S G 1 _ 2 0 1 1 1 1 1 3 D 0 1 D _ T 0 0 0 0 _ L 2 S G _ C L P R Q _ 1 0 0 1
        Example GC1SG1_20111113D01D_T0000_L2SG_CLPRQ_1001

              f"GC1SG1_20180901A01D_D0000_3MSG_EVI_F_2000.h5"
    """
    # if name[20] == "_":
    #     granule_type = "area_global"
    #     sensing_time = datetime.strptime(name[7:15], "%Y%m%d")
    # else:
    #     granule_type = "scene"
    #     sensing_time = datetime.strptime(name[7:20], "%Y%m%d%H%M%S")
    time_unit = (
        time_units[product_definition["time_unit"]]
        if product_definition["time_unit"]
        else ".*"
    )
    projection = (
        projections[product_definition["projection"]]
        if product_definition["projection"]
        else ".*"
    )
    tiles = "(" + "|".join(tiles) + ")" if tiles else ".*"
    level = (
        processing_level[product_definition["product"].split(".")[0]]
        if product_definition["product"]
        else ".*"
    )
    product_name = (
        product_definition["product"].split(".")[2]
        if product_definition["product"]
        else ".*"
    )
    resolution = (
        resolutions[product_definition["resolution"]]
        if product_definition["resolution"]
        else ".*"
    )
    regex_dict = {
        "area_global": re.compile(
            f".*_.*{time_unit}_{projection}{tiles}_{level}.*_{product_name}{resolution}_.*"
        ),
        "scene": re.compile(
            f".*_.*{time_unit}_{projection}{tiles}_{level}.*_{product_name}{resolution}_.*"
        ),
    }
    return regex_dict


def get_product_definition(user_options):
    """Extract product components from user options"""
    #
    if user_options["predefined_product"]:
        if user_options["predefined_product"] not in predefined_products:
            gscript.fatal(_(
                "No predefined product <{}> available".format(
                    user_options["predefined_product"]
                )
            ))
        product_definition = predefined_products[user_options["predefined_product"]]
    else:
        product_definition = {}
        for comp in url_components + ["time_unit", "resolution", "projection"]:
            product_definition[comp] = user_options[comp]
    return product_definition
