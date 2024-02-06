#!/usr/bin/env python3

"""
 MODULE:      i.sentinel3.import
 AUTHOR(S):   Stefan Blumentrath
 PURPOSE:     Import and pre-process Sentinel-3 data from the Copernicus program
 COPYRIGHT:   (C) 2024 by Norwegian Water and ENergy Directorate, Stefan Blumentrath,
              and the GRASS development team

              This program is free software under the GNU General Public
              License (>=v2). Read the file COPYING that comes with GRASS
              for details.
"""

# %Module
# % description: Import and pre-process Sentinel-3 data from the Copernicus program
# % keyword: imagery
# % keyword: satellite
# % keyword: Sentinel
# % keyword: import
# % keyword: optical
# % keyword: thermal
# %end

# %option
# % key: input
# % label: Sentinel-3 input data
# % description: Either a (commaseparated list of) path(s) to Sentinel-3 zip files or a textfile with such paths (one per row)
# % required: yes
# % multiple: yes
# %end

# %option
# % key: product_type
# % multiple: no
# % options: S3SL1RBT,S3SL2LST
# % answer: S3SL2LST
# % description: Sentinel-3 product type to import (currently, only S3SL1RBT and S3SL2LST are supported)
# % required: yes
# %end

# %option
# % key: bands
# % multiple: yes
# % answer: all
# % required: yes
# % description: Data bands to import (e.g. LST, default is all available)
# %end

# %option
# % key: anxillary_bands
# % multiple: yes
# % answer: all
# % required: no
# % description: Anxillary data bands to import (e.g. LST_uncertainty, default is all available)
# %end

# %option
# % key: flag_bands
# % multiple: yes
# % answer: all
# % required: no
# % description: Quality flag bands to import (e.g. bayes_in, default is all available)
# %end

# %option
# % key: basename
# % description: Basename used as prefix for map names (default is derived from the input file(s))
# % required: no
# %end

# %option G_OPT_F_OUTPUT
# % key: register_output
# % description: Name for output file to use with t.register
# % required: no
# %end

# %option G_OPT_M_DIR
# % key: metadata
# % description: Name of directory into which Sentinel metadata json dumps are saved
# % required: no
# %end

# %option G_OPT_M_NPROCS
# %end

# %option
# % key: memory
# % type: integer
# % required: no
# % multiple: no
# % label: Maximum memory to be used (in MB)
# % description: Cache size for raster rows
# % answer: 300
# %end

# %flag
# % key: a
# % description: Apply cloud mask before import (can significantly speed up import)
# % guisection: Settings
# %end

# %flag
# % key: c
# % description: Import LST in degree celsius (default is kelvin)
# % guisection: Settings
# %end

# %flag
# % key: d
# % description: Import data with decimals as double precision
# % guisection: Settings
# %end

# %flag
# % key: e
# % description: Import also elevation from geocoding of stripe
# % guisection: Settings
# %end

# %flag
# % key: i
# % description: Do not interpolate imported bands
# % guisection: Settings
# %end

# %flag
# % key: j
# % description: Write metadata json for each band to LOCATION/MAPSET/cell_misc/BAND/description.json
# % guisection: Settings
# %end

# %flag
# % key: o
# % description: Process oblique view (default is nadir)
# % guisection: Settings
# %end

# %flag
# % key: p
# % description: Print raster data to be imported and exit
# % guisection: Print
# %end

# %flag
# % key: r
# % description: Rescale radiance bands to reflectance
# % guisection: Settings
# %end

# %rules
# % excludes: -p,register_output
# %end


# ToDo
# - handle valid range according to: https://docs.unidata.ucar.edu/netcdf-c/current/attribute_conventions.html
# - add evtl. cleaning of external, temporary files
# - Adjust region bounds across tracks (pixel alignment may differ between tracks)
# - Add orphaned pixels
# - parallelize NC conversion


import atexit
import json
import os
import re
import sys

from collections import OrderedDict
from datetime import datetime
from itertools import chain
# from multiprocessing import Pool
from pathlib import Path
from zipfile import ZipFile

import grass.script as gs
from grass.pygrass.modules import Module, MultiModule, ParallelModuleQueue
from grass.temporal.datetime_math import (
    # adjust_datetime_to_granularity,
    create_suffix_from_datetime,
    # datetime_to_grass_datetime_string as grass_timestamp,
)
# from grass.temporal.temporal_granularity import check_granularity_string

S3_SOLAR_FLUX = {
    "S3SL1RBT": {
        "band": "{}_solar_irradiance_{}",
        "nc_file": "{}_quality_{}.nc",
        # See: https://github.com/senbox-org/s3tbx/blob/master/s3tbx-rad2refl/src/main/java/org/esa/s3tbx/processor/rad2refl/Rad2ReflConstants.java
        "defaults": {
            "S1": 1837.39,
            "S2": 1525.94,
            "S3": 956.17,
            "S4": 365.90,
            "S5": 248.33,
            "S6": 78.33,
        },
    },
    "S3SL2LST": None,
}

S3_SUPPORTED_PRODUCTS = ["S3SL1RBT", "S3SL2LST"]

S3_FILE_PATTERN = {
    "S3OL1ERF": None,
    "S3SL1RBT": "S3*SL_1_RBT*.zip",
    "S3SL2LST": "S3*SL_2_LST*.zip",
}

S3_SUN_PARAMTERS = {
    "S3OL1ERF": None,
    "S3SL1RBT": {
        "geocoding": {
            "nc_file": "geodetic_tx.nc",
            "bands": {"lat": "latitude_tx", "lon": "longitude_tx"},
        },
        "sun_bands": {
            "nc_file": "geometry_t{}.nc",
            "bands": {
                "solar_azimuth": "solar_azimuth_t{}",
                "solar_zenith": "solar_zenith_t{}",
            },
        },
    },
    "S3SL2LST": None,
}

S3_GRIDS = {
    "S3OL1ERF": None,
    "S3SL2LST": {"i": 1000.0},
    "S3SL1RBT": {
        "a": 500.0,  # Stripe A
        "b": 500.0,  # Stripe B
        "c": 1000.0,  # TDI grid
        "f": 500.0,  # F channel grid
        "i": 1000.0,  # 1km grid
    },
}

# Default view is n (nadir), o = oblique
S3_VIEWS = {
    "S3OL1ERF": None,
    "S3SL1RBT": ["o", "n"],
    "S3SL2LST": ["n"],
}

S3_RADIANCE_ADJUSTMENT = {
    "S3_PN_SLSTR_L1_08": {
        "o": {
            # Nadir
            "S1": 0.97,
            "S2": 0.98,
            "S3": 0.98,
            "S5": 1.11,
            "S6": 1.13,
        },
        "n": {
            # Oblique
            "S1": 0.94,
            "S2": 0.95,
            "S3": 0.95,
            "S5": 1.04,
            "S6": 1.07,
        },
    }
}

S3_BANDS = {
    "bands": {
        "S3OL1ERF": None,
        "S3SL1RBT": {
            "F1": {
                "geometries": ("f",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "BT",
            },
            "F2": {
                "geometries": ("i",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "BT",
            },
            "S1": {
                "geometries": ("a",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S2": {
                "geometries": ("a",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S3": {
                "geometries": ("a",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S4": {
                "geometries": ("a", "b"),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S5": {
                "geometries": ("a", "b"),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S6": {
                "geometries": ("a", "b"),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "radiance",
                "exception": "{}_{}_{}",
            },
            "S7": {
                "geometries": ("i",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "BT",
            },
            "S8": {
                "geometries": ("i",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "BT",
            },
            "S9": {
                "geometries": ("i",),
                "nc_file": "{}_{}_{}.nc",
                "full_name": "{}_{}_{}",
                "types": "BT",
            },
        },
        "S3SL2LST": {
            "LST": {
                "geometries": ("i",),
                "nc_file": "LST_{}.nc",
                "full_name": "{}",
                "types": None,
                "exception": "exception",
            },
            "LST_uncertainty": {
                "geometries": ("i",),
                "nc_file": "LST_{}.nc",
                "full_name": "{}",
                "types": None,
                "exception": "exception",
            },
        },
    },
    "flags": {
        "S3OL1ERF": None,
        "S3SL1RBT": {
            "bayes": {
                "geometries": ("a", "b", "f", "i"),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "cloud": {
                "geometries": ("a", "b", "f", "i"),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "confidence": {
                "geometries": ("a", "b", "f", "i"),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "pointing": {
                "geometries": ("a", "b", "f", "i"),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "probability_cloud_dual": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "probability_cloud_single": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
        },
        "S3SL2LST": {
            "bayes": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "cloud": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "confidence": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "probability_cloud_dual": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
            "probability_cloud_single": {
                "geometries": ("i",),
                "nc_file": "flags_{}.nc",
                "full_name": "{}_{}",
                "types": None,
            },
        },
    },
    "anxillary": {
        "S3OL1ERF": None,
        "S3SL1RBT": None,
        "S3SL2LST": {
            "biome": {
                "geometries": ("i",),
                "nc_file": "LST_ancillary_ds.nc",
                "full_name": "{}",
                "types": None,
            },
            "fraction": {
                "geometries": ("i",),
                "nc_file": "LST_ancillary_ds.nc",
                "full_name": "{}",
                "types": None,
            },
            "NDVI": {
                "geometries": ("i",),
                "nc_file": "LST_ancillary_ds.nc",
                "full_name": "{}",
                "types": None,
            },
            "TCWV": {
                "geometries": ("i",),
                "nc_file": "LST_ancillary_ds.nc",
                "full_name": "{}",
                "types": None,
            },
        },
    },
}

# GRASS map precision
DTYPE_TO_GRASS = {
    "float32": "FCELL",
    "uint16": "CELL",
    "uint8": "CELL",
    "float64": "DCELL",
}

OVERWRITE = gs.overwrite()

GISENV = gs.gisenv()
GISDBASE = GISENV["GISDBASE"]
TMP_FILE = Path(gs.tempfile(create=False))
TMP_FILE.mkdir(exist_ok=True, parents=True)
TMP_NAME = gs.tempname(12)

MAPSET = None
SUN_ZENITH_ANGLE = None


# From r.import

# initialize global vars
TMP_REG_NAME = None
TMPLOC = None
SRCGISRC = None
GISDBASE = None
TMP_REG_NAME = None


def cleanup():
    """Remove all temporary data"""
    # remove temporary maps
    if TMP_FILE:
        gs.try_remove(TMP_FILE)
    # remove temp location
    if TMPLOC:
        gs.try_rmdir(os.path.join(GISDBASE, TMPLOC))
    if SRCGISRC:
        gs.try_remove(SRCGISRC)
    if (
        TMP_REG_NAME
        and gs.find_file(
            name=TMP_REG_NAME, element="vector", mapset=gs.gisenv()["MAPSET"]
        )["fullname"]
    ):
        gs.run_command(
            "g.remove", type="vector", name=TMP_REG_NAME, flags="f", quiet=True
        )


def create_tmp_location(grass_env=gs.gisenv()):
    """Create a temporary location"""
    global GISDBASE, TMPLOC, SRCGISRC
    GISDBASE = grass_env["GISDBASE"]
    TMPLOC = gs.append_node_pid("tmp_s3_import_location")
    if not (Path(GISDBASE) / TMPLOC).exists():
        gs.run_command("g.proj", flags="c", location=TMPLOC, epsg=4326)
    SRCGISRC, src_env = gs.create_environment(GISDBASE, TMPLOC, "PERMANENT")
    return src_env


def np_as_scalar(var):
    """Return a numpy object as scalar"""
    if type(var).__module__ == np.__name__:
        if var.size > 1:
            return str(var)
        return var.item()
    return var


def write_metadata(json_dict, metadatajson):
    """Write extended map metadata to JSON file"""
    gs.verbose(_("Writing metadata to maps..."))
    with open(metadatajson, "w", encoding="UTF8") as outfile:
        json.dump(json_dict, outfile)


def write_register_file(filename, register_input):
    """Write file to register resulting maps"""
    gs.verbose(_("Writing register file <{}>...").format(filename))
    with open(filename, "w", encoding="UTF8") as register_file:
        register_file.write("\n".join(chain(*register_input)))


def convert_units(np_column, from_u, to_u):
    """Converts a numpy column from one unit to
    another, convertibility needs to be checked beforehand"""
    try:
        from cf_units import Unit
    except ImportError:
        gs.fatal(
            _(
                "Could not import cf_units. Please install it with:\n"
                "'pip install cf_units'!"
            )
        )

    try:
        converted_col = Unit(from_u).convert(np_column, Unit(to_u))
    except ValueError:
        gs.fatal(
            _(
                "Warning: Could not convert units from {from_u} to {to_u}.").format(
                    from_u=from_u, to_u=to_u
                )
            )
        converted_col = np_column

    return converted_col


def extend_region(region_dict, additional_region_dict):
    """Extend region bounds in region_dict to cover also the additional_region_dict"""
    region_dict["n"] = max(region_dict["n"], additional_region_dict["n"])
    region_dict["e"] = max(region_dict["e"], additional_region_dict["e"])
    region_dict["s"] = min(region_dict["s"], additional_region_dict["s"])
    region_dict["w"] = min(region_dict["w"], additional_region_dict["w"])
    # region_dict["nsres"] = (region_dict["nsres"] + additional_region_dict["nsres"]) / 2.0
    # region_dict["ewres"] = (region_dict["ewres"] + additional_region_dict["ewres"]) / 2.0
    return region_dict


def parse_s3_file_name(file_name):
    """Extract info from file name according to naming onvention:
    https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-3-slstr/naming-convention
    Assumes that file name is checked to be a valid / supported Sentinel-3 file name
    :param file_name: string representing the file name of a Senintel-3 scene
    """
    try:
        return {
            "mission_id": file_name[0:3],
            "instrument": file_name[4:6],
            "level": file_name[7],
            "product": file_name[9:12],
            "start_time": datetime.strptime(file_name[16:31], "%Y%m%dT%H%M%S"),
            "end_time": datetime.strptime(file_name[32:47], "%Y%m%dT%H%M%S"),
            "ingestion_time": datetime.strptime(file_name[48:63], "%Y%m%dT%H%M%S"),
            "duration": file_name[64:68],
            "cycle": file_name[69:72],
            "relative_orbit": file_name[73:76],
            "frame": file_name[77:81],
        }
    except ValueError:
        gs.fatal(_("{} is not a supported Sentinel-3 scene").format(str(file_name)))


def group_scenes(s3_files, granularity="1 day"):
    """
    Group scenes by information from the file name:
     1. mission ID
     2. product type
     3. temporal granule
     4. duration
     5. cycle
     6. relative orbit
     : param s3_filesv: list of pathlib.Path objects with Sentine1-3 files
    """
    groups = {}
    for sfile in s3_files:
        s3_name_dict = parse_s3_file_name(sfile.name)
        group_id = "_".join(
            [
                s3_name_dict["mission_id"],
                options["product_type"][2:],
                create_suffix_from_datetime(
                    s3_name_dict["start_time"], granularity
                ).replace("_", ""),
                s3_name_dict["duration"],
                s3_name_dict["cycle"],
                s3_name_dict["relative_orbit"],
            ]
        )
        if group_id in groups:
            groups[group_id]["files"].append(sfile)
            if s3_name_dict["start_time"] < groups[group_id]["start_time"]:
                groups[group_id]["start_time"] = s3_name_dict["start_time"]
            if s3_name_dict["end_time"] > groups[group_id]["end_time"]:
                groups[group_id]["end_time"] = s3_name_dict["end_time"]
            groups[group_id]["frames"].append(s3_name_dict["frame"])
        else:
            groups[group_id] = {
                "files": [sfile],
                "start_time": s3_name_dict["start_time"],
                "end_time": s3_name_dict["end_time"],
                "frames": [s3_name_dict["frame"]],
            }
    return groups


def extract_file_info(s3_files, basename=None):
    """Extract information from file name according to naming conventions"""
    result_dict = {}
    for s3_file in s3_files:
        file_info = parse_s3_file_name(s3_file.name)
        if not result_dict:
            result_dict = file_info
            result_dict["duration"] = {file_info["duration"]}
            result_dict["cycle"] = {file_info["cycle"]}
            result_dict["relative_orbit"] = {file_info["relative_orbit"]}
            result_dict["frame"] = {file_info["frame"]}
        else:
            if file_info["mission_id"] != result_dict["mission_id"]:
                result_dict["mission_id"] = "S3_"
            result_dict["start_time"] = min(
                result_dict["start_time"], file_info["start_time"]
            )
            result_dict["end_time"] = max(
                result_dict["end_time"], file_info["end_time"]
            )
            result_dict["duration"].add(file_info["duration"])
            result_dict["cycle"].add(file_info["cycle"])
            result_dict["relative_orbit"].add(file_info["relative_orbit"])
            result_dict["frame"].add(file_info["frame"])
    for key in ["duration", "cycle", "relative_orbit"]:
        if len(result_dict[key]) > 1:
            gs.warning(
                _("Merging {key}s {values}").format(
                    key=key, values=", ".join(result_dict[key])
                )
            )
    if len(result_dict["mission_id"]) == "3_":
        gs.warning(
            _("Merging Seninel-3A and Seninel-3B data").format(
                key=key, values=", ".join(result_dict[key])
            )
        )
    if not basename:
        basename = "_".join(
            [
                result_dict["mission_id"],
                result_dict["instrument"],
                result_dict["level"],
                result_dict["product"],
                result_dict["start_time"].strftime("%Y%m%d%H%M%S"),
                result_dict["end_time"].strftime("%Y%m%d%H%M%S"),
                list(result_dict["duration"])[0],
                list(result_dict["cycle"])[0],
                list(result_dict["relative_orbit"])[0],
            ]
        )
    return {basename: result_dict}


def adjust_region_env(reg, coords):
    """Get region bounding box of intersecting area with
    coordinates aligned to the current region"""
    coords_min = np.min(coords, axis=0)
    coords_max = np.max(coords, axis=0)

    diff = coords_max[0:2] / np.array([float(reg["ewres"]), float(reg["nsres"])])
    coords_max_aligned = diff.astype(int) * np.array(
        [float(reg["ewres"]), float(reg["nsres"])]
    )

    diff = coords_min[0:2] / np.array([float(reg["ewres"]), float(reg["nsres"])])
    coords_min_aligned = diff.astype(int) * np.array(
        [float(reg["ewres"]), float(reg["nsres"])]
    )

    new_bounds = {}
    new_bounds["e"], new_bounds["n"] = tuple(
        np.min(
            np.vstack(
                (coords_max_aligned, np.array([reg["e"], reg["n"]]).astype(float))
            ),
            axis=0,
        ).astype(str)
    )
    new_bounds["w"], new_bounds["s"] = tuple(
        np.max(
            np.vstack(
                (coords_min_aligned, np.array([reg["w"], reg["s"]]).astype(float))
            ),
            axis=0,
        ).astype(str)
    )

    if float(new_bounds["s"]) >= float(new_bounds["n"]) or float(
        new_bounds["w"]
    ) >= float(new_bounds["e"]):
        return None

    compute_env = os.environ.copy()
    compute_env["GRASS_region"] = gs.region_env(**new_bounds)

    return compute_env


def get_geocoding(zip_file, root, geo_bands_dict, region_bounds=None):
    """Get ground control points from NetCDF file"""
    member = str(root / geo_bands_dict["nc_file"])
    nc_file_path = zip_file.extract(member, path=TMP_FILE)
    with Dataset(nc_file_path) as nc_file_open:
        nc_bands = OrderedDict()
        print(nc_file_open.resolution)
        resolution = nc_file_open.resolution.strip(" ").split(" ")[1:3]

        for band_id, band in geo_bands_dict["bands"].items():
            if band not in nc_file_open.variables:
                gs.fatal(
                    _(
                        "{s3_file} does not contain a container {container} with band {band}").format(
                            s3_file=str(Path(nc_file_path).parent.name),
                            container=geo_bands_dict["nc_file"],
                            band=", ".join(band),
                        )
                    )

            # Add band to dict
            nc_bands[band_id] = nc_file_open[band][:]

        # Create initial mask
        mask = nc_bands["lat"][:].mask

        if region_bounds:
            # Mask to region
            mask = np.ma.mask_or(
                mask,
                np.ma.masked_outside(
                    nc_bands["lon"],
                    float(region_bounds["ll_w"]),
                    float(region_bounds["ll_e"]),
                ).mask,
            )
            mask = np.ma.mask_or(
                mask,
                np.ma.masked_outside(
                    nc_bands["lat"],
                    float(region_bounds["ll_s"]),
                    float(region_bounds["ll_n"]),
                ).mask,
            )

            if mask.all():
                gs.fatal(_("No valid pixels inside computational region"))

    return nc_bands, mask, resolution


def setup_import_multi_module(
    tmp_ascii,
    mapname,
    zrange=None,
    val_col=None,
    data_type=None,
    method="mean",
    solar_flux=None,
    rules=None,
    # support_kwargs=None,
    # env_=None,
):
    """Setup GRASS GIS moduls for importing S3 bands"""
    # Basic import module
    modules = [
        Module(
            "r.in.xyz",
            input=str(tmp_ascii),
            output=f"{TMP_NAME}_{mapname}",
            method=method,
            separator=",",
            x=1,
            y=2,
            # Array contains a column z at position 3 (with all 0)
            # after coordinate transformation
            z=val_col,
            flags="i",
            type=data_type,
            zrange=zrange,
            percent=100,
            run_=False,
            quiet=gs.verbosity() <= 2,
            overwrite=OVERWRITE,
            # env_=env_,
        )
    ]
    # Interpolation for missing pixels
    nbh_function = "nmedian" if method == "mean" else "nmode"
    neighborhoods = ", ".join(
        [
            f"{TMP_NAME}_{mapname}{nbh}"
            for nbh in [
                "[0,1]",
                "[1,1]",
                "[1,0]",
                "[1,-1]",
                "[0,-1]",
                "[-1,-1]",
                "[-1,0]",
                "[-1,1]",
            ]
        ]
    )
    mc_interpolate_template = f"{{mapname}}=if(isnull({TMP_NAME}_{mapname}), {nbh_function}({neighborhoods}), {TMP_NAME}_{mapname})"

    # Add conversion from radiance to reflectance if requested and relevant
    if solar_flux:
        mc_interpolate = mc_interpolate_template.format(
            mapname=f"{TMP_NAME}_{mapname}_rad"
        )
        mapname_reflectance = mapname.replace("radiance", "reflectance")
        mc_expression = f"eval({mc_interpolate})\n{mapname_reflectance}={TMP_NAME}_{mapname}_rad * {np.pi} / {solar_flux} / cos({SUN_ZENITH_ANGLE})"
        mapname = mapname_reflectance
    else:
        mc_expression = mc_interpolate_template.format(mapname=mapname)

    # Create mapcalc module
    modules.append(
        Module(
            "r.mapcalc",
            expression=mc_expression,
            quiet=gs.verbosity() <= 2,
            overwrite=OVERWRITE,
            run_=False,
            # env_=env_,
        )
    )
    # if support_kwargs:
    #     modules.extend(
    #         [
    #             Module(
    #                 "r.support",
    #                 **support_kwargs,
    #                 quiet=True,
    #                 run_=False,
    #             ),
    #             Module(
    #                 "r.timestamp",
    #                 map=mapname,
    #                 date=start_time,
    #                 quiet=True,
    #                 run_=False,
    #             ),
    #         ]
    #     )

    # Add categories (for flag datasets)
    if rules:
        modules.append(
            Module(
                "r.category",
                quiet=True,
                map=mapname,
                rules="-",
                stdin_=rules,
                separator=":",
                run_=False,
            )
        )
    return modules


def get_file_metadata(nc_dataset):
    """Collect metadata from NetCDF file"""
    metadata = {
        attr: np_as_scalar(nc_dataset.getncattr(attr))
        for attr in [
            "title",
            "creation_time",
            "absolute_orbit_number",
            "track_offset",
            "start_offset",
            "institution",
            "references",
            "resolution",
            "source",
            "contact",
            "comment",
            "history",
            "processing_baseline",
            "product_name",
        ]
        if attr in nc_dataset.ncattrs()
    }

    metadata["start_time"] = parse_timestr(nc_dataset.start_time)
    metadata["end_time"] = parse_timestr(nc_dataset.stop_time)

    return metadata


def get_band_metadata(
    band, nc_variable, fmt, file_metadata=None, basename=None, module_flags=None
):
    """Extract band metadata from NetCDF variable"""
    metadata = file_metadata.copy()
    # Collect metadata
    band_attrs = nc_variable.ncattrs()

    # Define variable name
    varname_short = band.full_name
    datatype = str(nc_variable[:].dtype)

    # Define map name
    mapname = f"{basename}_{varname_short}"
    metadata["mapname"] = mapname

    band_title = nc_variable.long_name if "long_name" in band_attrs else band.full_name

    # Define unit
    unit = nc_variable.units if "units" in band_attrs else None
    unit = "degree_celsius" if band.band_id.startswith("LST") and module_flags["c"] else unit
    metadata["unit"] = unit

    # Define datatype and import method
    if datatype not in DTYPE_TO_GRASS:
        gs.fatal(
            _("Unsupported datatype {dt} in band {band}").format(
                dt=datatype, band=band.full_name
            )
        )
    if datatype in ["uint8", "uint16"]:
        method = "max"  # Unfortunately there is no "mode" in r.in.xyz
        fmt += ",%i"
    else:
        method = "mean"
        fmt += ",%.12f"
    metadata["method"] = method
    metadata["datatype"] = DTYPE_TO_GRASS[datatype]

    # Compile description
    for time_reference in ["start_time", "end_time"]:
        metadata[time_reference] = metadata[time_reference].isoformat()
    description = json.dumps(metadata, separators=["\n", ": "]).lstrip("{").rstrip("}")

    # Get max and min values
    if "valid_max" in band_attrs:
        min_val = nc_variable.valid_min
        max_val = nc_variable.valid_max
        if "scale_factor" in band_attrs:
            min_val = min_val * nc_variable.scale_factor
            max_val = max_val * nc_variable.scale_factor
        if "add_offset" in band_attrs:
            min_val = min_val + nc_variable.add_offset
            max_val = max_val + nc_variable.add_offset
        description += f"\n\nvalid_min: {min_val}\nvalid_max: {max_val}"
    else:
        min_val = nc_variable[:].min()
        max_val = nc_variable[:].max()
    metadata["description"] = description

    # Define valid input range
    if "_FillValue" in band_attrs:
        fill_val = nc_variable._FillValue if "_FillValue" in band_attrs else None
        if fill_val > 0:
            zrange = [
                fill_val - 1 if not min_val else np.max([fill_val - 1, min_val]),
                max_val,
            ]
        else:
            zrange = [
                fill_val + 1 if not min_val else np.max([fill_val + 1, min_val]),
                max_val,
            ]
    elif "flag_values" in band_attrs:
        zrange = [
            min(nc_variable.flag_values),
            max(nc_variable.flag_values),
        ]
    else:
        zrange = None
    metadata["zrange"] = zrange
    metadata["support_kwargs"] = {
        "map": mapname,
        "title": f"{band_title} from {metadata['title']}",
        "history": None,
        "units": unit,
        "source1": metadata["product_name"],
        "source2": metadata["history"],
        "description": description,
        "semantic_label": f"S3_{varname_short}",
    }

    return metadata, fmt


def transform_coordinates(coordinates):
    """Tranforms a numy array with coordinates to
    projection of the current location"""

    # Create source coordinate reference
    s_srs = osr.SpatialReference()
    s_srs.ImportFromEPSG(4326)

    # Create target coordinate reference
    t_srs = osr.SpatialReference()
    t_srs.ImportFromWkt(gs.read_command("g.proj", flags="fw"))

    # Initialize osr transformation
    transform = osr.CoordinateTransformation(s_srs, t_srs)

    return (
        coordinates[:, [1, 0]]
        if s_srs.IsSame(t_srs)
        else np.array(transform.TransformPoints(coordinates))[:, [0, 1]]
    )


def write_xyz(tmp_ascii, nc_bands, mask, fmt=None, project=True):
    """Write temporary XYZ ascii file"""
    # Extract grid coordinates
    lon = np.ma.masked_where(mask, nc_bands["lon"][:]).compressed()
    lat = np.ma.masked_where(mask, nc_bands["lat"][:]).compressed()

    if project:
        # Project coordinates
        np_output = transform_coordinates(
            np.dstack((lat, lon)).reshape(lat.shape[0], 2)
        )
    else:
        np_output = np.hstack((lon[:, None], lat[:, None]))
    # Fetch, mask and stack requested bands
    for band in nc_bands:
        if band in ["lat", "lon"]:
            continue
        add_array = nc_bands[band][:]
        if np.ma.is_masked(add_array):
            add_array = add_array.filled()
        np_output = np.hstack(
            (np_output, np.ma.masked_where(mask, add_array).compressed()[:, None])
        )
        print(
            band,
            np.min(add_array),
            np.max(np.ma.masked_where(mask, add_array).compressed()[:, None]),
        )
        print(
            band,
            np.min(np.ma.masked_where(mask, add_array).compressed()[:, None]),
            np.max(np.ma.masked_where(mask, add_array).compressed()[:, None]),
        )
    # Write to temporary file
    if Path(tmp_ascii).exists():
        with open(tmp_ascii, "ab") as tmp_ascii_file:
            np.savetxt(tmp_ascii_file, np_output, delimiter=",", fmt=fmt)
    else:
        np.savetxt(tmp_ascii, np_output, delimiter=",", fmt=fmt)

    stripe_reg = {  #
        "n": np.ma.max(np_output[:, 1]),  # + (nsres / 2.0),
        "s": np.ma.min(np_output[:, 1]),  # - (nsres / 2.0),
        "e": np.ma.max(np_output[:, 0]),  # + (ewres / 2.0),
        "w": np.ma.min(np_output[:, 0]),  # - (ewres / 2.0),
        # "nsres": nsres,
        # "ewres": ewres,
    }
    return stripe_reg


def import_s3(s3_file, kwargs, s3_product=None):
    """Import Sentinel-3 netCDF4 data"""
    # Unpack dictionary variables
    rmap = list(kwargs["meta_dict"].keys())[0]
    region_bounds = kwargs["reg_bounds"]
    # current_reg = kwargs["current_reg"]
    mod_flags = kwargs["mod_flags"]
    json_standard_folder = kwargs["json_standard_folder"]
    module_queue = {}
    region_dicts = {}
    register_strings = {}

    with ZipFile(s3_file) as zip_file:
        members = zip_file.namelist()
        root = members[0].rsplit(".SEN3", maxsplit=1)[0]
        root = Path(f"{root}.SEN3")
        # Check if solar flux raster band is required
        if mod_flags["r"]:
            tmp_ascii_sun = TMP_FILE / f"{rmap}_sun_parameters.txt"
            module_queue, sun_region_dict = s3_product.get_sun_parameters(
                zip_file, root, rmap, tmp_ascii_sun, region_bounds
            )
            region_dicts["sun_parameters"] = sun_region_dict
        for stripe, stripe_dict in s3_product.requested_stripe_content.items():
            # Could be parallelized!!!
            # Get geocoding (lat/lon, elevation) and initial mask
            tmp_ascii = TMP_FILE / f"{rmap}_{stripe}.txt"
            nc_bands, mask, resolution = get_geocoding(
                zip_file, root, stripe_dict["geocoding"], region_bounds=region_bounds
            )
            fmt = ",".join(["%.12f"] * len(nc_bands))
            for nc_file, bands in stripe_dict["containers"].items():
                (
                    nc_bands,
                    module_queue,
                    register_output,
                    fmt,
                ) = s3_product.process_nc_file(
                    zip_file,
                    root,
                    (nc_file, bands),
                    nc_bands,
                    rmap,
                    module_queue,
                    mod_flags,
                    tmp_ascii,
                    fmt=fmt,
                    json_standard_folder=json_standard_folder,
                )
                register_strings[nc_file] = register_output
            region_dicts[stripe] = write_xyz(
                tmp_ascii, nc_bands, mask, fmt=fmt, project=True
            )
            region_dicts[stripe]["ewres"], region_dicts[stripe]["nsres"] = float(
                resolution[0]
            ), float(resolution[1])

    return module_queue, register_strings, region_dicts


class S3Product:
    """Class to provide information necessary to pre-process Senintel-3 data products
    level 1 and 2"""

    def __init__(
        self, product_type, view="n", bands=None, flag_bands=None, anxillary_bands=None
    ):
        self.product_type = product_type
        self.available_views = S3_VIEWS[product_type]
        self.available_bands = list(S3_BANDS["bands"][product_type].keys())
        self.available_flag_bands = list(S3_BANDS["flags"][product_type].keys())
        self.available_anxillary_bands = (
            list(S3_BANDS["anxillary"][product_type].keys())
            if S3_BANDS["anxillary"][product_type]
            else None
        )
        self.view = self._check_view(view)
        self.bands = self._check_bands(bands)
        self.flag_bands = self._check_bands(flag_bands, band_type="flag_bands")
        self.anxillary_bands = self._check_bands(
            anxillary_bands, band_type="anxillary_bands"
        )
        self.grids = S3_GRIDS[product_type]
        self.requested_stripe_content = self._collect_requested_stripe_content()
        self.file_pattern = S3_FILE_PATTERN[product_type]

    def __str__(self):
        return json.dumps(self.__repr__(), indent=2)

    def __repr__(self):
        class_dict = self.__dict__.copy()
        for band_type in ["bands", "flag_bands", "anxillary_bands"]:
            bands_of_type = getattr(self, band_type)
            print(bands_of_type)
            class_dict[band_type] = (
                {
                    band: description.__dict__ if description else None
                    for band, description in bands_of_type.items()
                }
                if bands_of_type
                else None
            )
        return json.dumps(class_dict, indent=2)

    def _check_view(self, view):
        if view not in self.available_views:
            gs.warning(_(
                "View {} not available for product type {}").format(
                    view, self.product_type
                )
            )
        return view

    def _check_bands(self, bands, band_type="bands"):
        band_types = {
            "bands": self.available_bands,
            "flag_bands": self.available_flag_bands,
            "anxillary_bands": self.available_anxillary_bands,
        }
        if not band_types[band_type]:
            return None
        if not bands:
            if not bands and band_type == "bands":
                gs.warning(_("At least one band band needs to be given"))
            return None
        if bands == "all":
            bands = band_types[band_type]
        band_objects = {}
        for band in bands:
            if band not in band_types[band_type]:
                gs.warning(_(
                    "Band {0} is not available in product_type {1}").format(
                        band, self.product_type
                    )
                )
            band_objects[band] = S3Band(self.product_type, band, use_b=False, view="n")
        return band_objects

    def _collect_requested_stripe_content(self):
        suffixes = {}
        for band_type in ["bands", "flag_bands", "anxillary_bands"]:
            selected_bands = getattr(self, band_type)
            if not selected_bands:
                continue
            for band_id, band_object in selected_bands.items():
                if band_object.suffix in suffixes:
                    if "containers" not in suffixes[band_object.suffix]:
                        suffixes[band_object.suffix]["containers"] = {}
                    if (
                        band_object.nc_file
                        in suffixes[band_object.suffix]["containers"]
                    ):
                        suffixes[band_object.suffix]["containers"][
                            band_object.nc_file
                        ].append(band_id)
                    else:
                        suffixes[band_object.suffix]["containers"][
                            band_object.nc_file
                        ] = [band_id]
                else:
                    suffixes[band_object.suffix] = {
                        "containers": {band_object.nc_file: [band_id]},
                        "geocoding": {
                            "nc_file": f"geodetic_{band_object.suffix}.nc",
                            "bands": {
                                "lat": f"latitude_{band_object.suffix}",
                                "lon": f"longitude_{band_object.suffix}",
                                "elevation": f"elevation_{band_object.suffix}",
                            },
                        },
                    }
        return suffixes

    def process_nc_file(
        self,
        zip_file,
        root,
        container_dict_items,
        nc_bands,
        prefix,
        module_queue,
        module_flags,
        tmp_ascii,
        fmt=None,
        json_standard_folder=None,
    ):
        """Extract requested bands as numpy arrays from NetCDF file and setup import modules"""
        meta_information = {}
        member = str(root / container_dict_items[0])
        nc_file_path = zip_file.extract(member, path=TMP_FILE)
        with Dataset(nc_file_path) as nc_file_open:
            file_metadata = get_file_metadata(nc_file_open)

            for band in container_dict_items[1]:
                # Check for band
                solar_flux = None
                if band in self.bands:
                    band = self.bands[band]
                    if band.solar_flux:
                        solar_flux = band.solar_flux["default"]
                elif band in self.flag_bands:
                    band = self.flag_bands[band]
                elif band in self.anxillary_bands:
                    band = self.anxillary_bands[band]

                if band.full_name not in nc_file_open.variables:
                    gs.fatal(
                        _(
                            "{s3_file} does not contain a container {container} with band {band}").format(
                                s3_file=str(root),
                                container=band.nc_file,
                                band=", ".join(band.full_name),
                            )
                        )

                nc_variable = nc_file_open[band.full_name]
                # Apply radiance adjustment
                if band.radiance_adjustment and module_flags["r"]:
                    add_array = nc_variable[:] * band.radiance_adjustment
                else:
                    add_array = nc_variable[:]

                # Collect band metadata
                metadata = get_band_metadata(
                    band,
                    nc_variable,
                    fmt,
                    file_metadata=file_metadata,
                    basename=prefix,
                    module_flags=module_flags,
                )
                fmt = metadata[1]
                if band.radiance_adjustment and module_flags["r"]:
                    # Get solar flux for band
                    metadata[0]["solar_flux"] = band.get_solar_flux(
                        zip_file, root,
                    )

                if band.exception:
                    if np.ma.is_masked(add_array):
                        add_array.mask = np.ma.mask_or(
                            add_array.mask, nc_file_open[band.exception][:] > 0
                        )
                    else:
                        add_array = np.ma.masked_array(
                            add_array, nc_file_open[band.exception][:] > 0
                        )
                if np.ma.is_masked(add_array):
                    add_array = add_array.filled()

                # Rescale temperature variables if requested
                if band.full_name.startswith("LST") and module_flags["c"]:
                    add_array = convert_units(add_array, "K", "degree_celsius")

                # Write metadata json if requested
                band_attrs = nc_file_open[band.full_name].ncattrs()
                if json_standard_folder:
                    write_metadata(
                        {
                            **{
                                a: np_as_scalar(nc_file_open.getncattr(a))
                                for a in nc_file_open.ncattrs()
                            },
                            **{"variable": band.full_name},
                            **{
                                a: np_as_scalar(
                                    nc_file_open[band.full_name].getncattr(a)
                                )
                                for a in nc_file_open[band.full_name].ncattrs()
                            },
                        },
                        json_standard_folder.joinpath(metadata[0]["mapname"] + ".json"),
                    )

                # Define categories for flag datasets
                rules = None
                if "flag_masks" in band_attrs:
                    rules = "\n".join(
                        [
                            ":".join(
                                [
                                    str(nc_file_open[band.full_name].flag_masks[idx]),
                                    label,
                                ]
                            )
                            for idx, label in enumerate(
                                nc_file_open[band.full_name].flag_meanings.split(" ")
                            )
                        ]
                    )
                # Setup import modules
                module_queue[band.full_name] = setup_import_multi_module(
                    tmp_ascii,
                    metadata[0]["mapname"],
                    zrange=metadata[0]["zrange"],
                    val_col=len(nc_bands) + 1,
                    data_type=metadata[0]["datatype"],
                    method=metadata[0]["method"],
                    # support_kwargs=metadata[0]["support_kwargs"],
                    solar_flux=solar_flux,
                    rules=rules,
                    # env_=None,
                )

                # Add array with invalid data masked to ordered dict of nc_bands
                nc_bands[band.full_name] = add_array
                meta_information[metadata[0]["mapname"]] = {
                    "rules": rules,
                    "start_time": metadata[0]["start_time"],
                    "end_time": metadata[0]["end_time"],
                    "support_kwargs": metadata[0]["support_kwargs"],
                }

            return nc_bands, module_queue, meta_information, fmt

    def get_sun_parameters(self, zip_file, root, prefix, tmp_ascii, region_bounds):
        """https://github.com/sertit/eoreader/blob/main/eoreader/products/optical/s3_slstr_product.py#L862"""

        # nc_file = sun_azimuth_dict["nc_file"]
        # Get values
        import_modules = {}
        fmt = "%.12f,%.12f"
        nc_bands, mask, resolution = get_geocoding(
            zip_file,
            root,
            S3_SUN_PARAMTERS[self.product_type]["geocoding"],
            region_bounds=region_bounds,
        )
        member = str(
            root
            / S3_SUN_PARAMTERS[self.product_type]["sun_bands"]["nc_file"].format(
                self.view
            )
        )
        nc_file_path = zip_file.extract(member, path=TMP_FILE)
        sun_parameter_nc = Dataset(nc_file_path)
        # sun_metadata = get_file_metadata(sun_parameter_nc)
        for band_id, band in S3_SUN_PARAMTERS[self.product_type]["sun_bands"][
            "bands"
        ].items():
            fmt += ",%.12f"
            sun_parameter_array = sun_parameter_nc[band.format(self.view)]
            nc_bands[band_id] = sun_parameter_array[:] * np.pi / 180.0
            map_name = f"{prefix}_{band_id}"
            if band_id == "solar_zenith":
                global SUN_ZENITH_ANGLE
                SUN_ZENITH_ANGLE = map_name
            # support_kwargs = {
            #     "map": map_name,
            #     "title": f"{sun_parameter_array.long_name} from {root}",
            #     # "history": nc_file_open.history,
            #     "units": sun_parameter_array.units,
            #     "source1": sun_parameter_nc.product_name,
            #     "source2": None,
            #     # "description": description,
            #     "semantic_label": f"S3_{sun_parameter_array.standard_name}",
            # }
            import_modules[band_id] = setup_import_multi_module(
                tmp_ascii,
                map_name,
                zrange=None,
                val_col=len(nc_bands),
                data_type="FCELL",
                method="mean",
                solar_flux=None,
           )

        # Write to temporary file
        sun_region_dict = write_xyz(tmp_ascii, nc_bands, mask, fmt=fmt, project=True)
        sun_region_dict["ewres"], sun_region_dict["nsres"] = float(
            resolution[0]
        ), float(resolution[1])

        return import_modules, sun_region_dict


class S3Band:
    """Class for properties and methods related to Sentinel-3 bands"""
    def __init__(
        self,
        product_type,
        band_id,
        use_b=False,
        view="n",
        radiance_adjustment="S3_PN_SLSTR_L1_08",
    ):
        self.band_id = band_id
        # self.product_type = product_type
        # self.use_b = use_b
        self.band_type = self._get_band_type(product_type)
        # Need to call in this order
        # self.view = view
        geometry = self._get_geometry(product_type, use_b)
        self.resolution = S3_GRIDS[product_type][geometry]
        self.suffix = f"{geometry}{view}"
        self.full_name = self._get_full_name(product_type)
        self.exception = self._get_exception_band(product_type)
        self.nc_file = self._get_nc_file(product_type)
        self.radiance_adjustment = self._get_radiance_adjustment(
            radiance_adjustment, view
        )
        self.solar_flux = self._get_solar_flux_dict_for_band(product_type)

    def __str__(self):
        return json.dumps(self.__repr__(), indent=2)

    def __repr__(self):
        return json.dumps(self.__dict__, indent=2)

    def _get_band_type(self, product_type):
        for band_type, s3_bands in S3_BANDS.items():
            if self.band_id in s3_bands[product_type]:
                return band_type
        gs.fatal(_("Oh no"))
        return None

    def _get_geometry(self, product_type, use_b):
        band_dict = S3_BANDS[self.band_type][product_type][self.band_id]
        return (
            "b"
            if use_b and "b" in band_dict["geometries"]
            else band_dict["geometries"][0]
        )

    def _get_full_name(self, product_type):
        band_dict = S3_BANDS[self.band_type][product_type][self.band_id]
        types = band_dict["types"]
        if self.band_type != "bands":
            return band_dict["full_name"].format(self.band_id, self.suffix)
        if types:
            return band_dict["full_name"].format(self.band_id, types, self.suffix)
        return band_dict["full_name"].format(self.band_id, self.suffix)

    def _get_nc_file(self, product_type):
        band_dict = S3_BANDS[self.band_type][product_type][self.band_id]
        types = band_dict["types"]
        if self.band_type == "flags":
            return band_dict["nc_file"].format(self.suffix)
        if types:
            return band_dict["nc_file"].format(self.band_id, types, self.suffix)
        return band_dict["nc_file"].format(self.band_id, self.suffix)

    def _get_radiance_adjustment(self, radiance_adjustment, view):
        if radiance_adjustment not in S3_RADIANCE_ADJUSTMENT:
            print("Oh no")
        if self.band_id in S3_RADIANCE_ADJUSTMENT[radiance_adjustment][view]:
            return S3_RADIANCE_ADJUSTMENT[radiance_adjustment][view][self.band_id]
        return None

    def _get_exception_band(self, product_type):
        if "exception" in S3_BANDS[self.band_type][product_type][self.band_id]:
            return S3_BANDS[self.band_type][product_type][self.band_id]["exception"].format(
                self.band_id, "exception", self.suffix
            )
        return None

    def _get_solar_flux_dict_for_band(self, product_type):
        if self.band_id in S3_SOLAR_FLUX[product_type]["defaults"]:
            return {
                "default": S3_SOLAR_FLUX[product_type]["defaults"][self.band_id],
                "band": S3_SOLAR_FLUX[product_type]["band"].format(
                    self.band_id, self.suffix
                ),
                "nc_file": S3_SOLAR_FLUX[product_type]["nc_file"].format(
                    self.band_id, self.suffix
                ),
            }
        return None

    def get_solar_flux(self, zip_file, root_path):
        """
        Get solar spectral flux in mW / (m^2 * sr * nm) for band

        Args:
            band_object (S3Band): Optical Band with radiance

        Returns:
            float: solar Flux

        """
        if not self.solar_flux:
            return None
        member = str(root_path / self.solar_flux["nc_file"])
        nc_file_path = zip_file.extract(member, path=TMP_FILE)
        sf_dataset = Dataset(nc_file_path)
        solar_flux = np.nanmean(sf_dataset[self.solar_flux["band"]])
        if np.isnan(solar_flux):
            solar_flux = self.solar_flux["default"]
        return float(solar_flux)

    def _get_band_geo_points(self, suffix):
        """Get ground control point references from suffix"""
        if self.suffix.startswith("t"):
            return None
        return {
            "nc_file": f"geodetic_{suffix}.nc",
            "lat": f"latitude_{suffix}",
            "lon": f"longitude_{suffix}",
            "elevation": f"elevation_{suffix}",
        }

    def adjust_radiance(self, np_band_array):
        """Get radiance adjustment for band object"""
        if not self.radiance_adjustment:
            return np_band_array * self.radiance_adjustment
        return np_band_array



def main():
    """Do the main work"""
    pattern = re.compile(S3_FILE_PATTERN[options["product_type"]])

    # check provided input
    s3_files = options["input"].split(",")
    if len(s3_files) == 1:
        if not re.match(pattern, s3_files[0]):
            try:
                s3_files = (
                    Path(s3_files[0]).read_text(encoding="UTF8").strip().split("\n")
                )
            except ValueError:
                gs.fatal(
                    _(
                        "Input <{}> is neither a supported Sentinel-3 scene nor a text file with scenes"
                    ).format(options["input"])
                )

    if not s3_files:
        gs.warning("No scenes found to process, please check input.")
        sys.exit()

    s3_files = [Path(scene) for scene in s3_files]
    for s3_scene in s3_files:
        if not s3_scene.exists():
            gs.fatal(_("Input file <{sn}> not found").format(str(sn=s3_scene)))
        if re.match(pattern, s3_scene.name):
            gs.fatal(
                _(
                    "Input <{sn}> is not a supported Sentinel-3 scene for the requested product type <{pt}>"
                ).format(sn=s3_scene, pt=options["product_type"])
            )

    if flags["f"]:
        global DTYPE_TO_GRASS
        DTYPE_TO_GRASS["float64"] = "FCELL"

    s3_product = S3Product(
        options["product_type"],
        view="o" if flags["o"] else "n",
        bands=options["bands"],
        flag_bands=options["flag_bands"],
        anxillary_bands=options["anxillary_bands"],
    )

    meta_info_dict = extract_file_info(s3_files, basename=options["basename"])
    # Group scenes by mission,
    # groups = group_scenes(s3_files, granularity="1 day")
    if gs.parse_command("g.proj", flags="g")["proj"] == "ll":
        gs.fatal(_("Running in lonlat location is not supported"))

    nprocs = int(options["nprocs"])

    # Get region bounds
    region_bounds = gs.parse_command("g.region", flags="ugb", quiet=True)
    current_region = gs.parse_command("g.region", flags="ug")
    # has_band_ref = float(gs.version()["version"][0:3]) >= 7.9

    json_standard_folder = None
    if flags["j"]:
        env = gs.gisenv()
        json_standard_folder = Path(env["GISDBASE"]).joinpath(
            env["LOCATION_NAME"], env["MAPSET"], "cell_misc"
        )
        if not json_standard_folder.exists():
            json_standard_folder.mkdir(parents=True, exist_ok=True)

    # Collect variables for import
    import_dict = {
        "reg_bounds": dict(region_bounds),
        "current_reg": dict(current_region),
        "mod_flags": flags,
        "json_standard_folder": json_standard_folder,
        "meta_dict": meta_info_dict,
    }

    if flags["p"]:
        print(
            "|".join(
                [
                    "product_file_name",
                    "nc_file_name",
                    "nc_file_title",
                    "nc_file_start_time",
                    "nc_file_creation_time",
                    "band",
                    "band_shape",
                    "band_title",
                    "band_standard_name",
                    "band_long_name",
                ]
            )
        )
        print("\n".join(chain(*import_result)))
        return 0

    module_queues = []
    register_strings = {}
    region_dicts = {}
    region_list = []

    for s3_file in s3_files:
        module_list, regs, region_dict = import_s3(s3_file, import_dict, s3_product=s3_product)
        module_queues.append(module_list)
        register_strings.update(regs)
        region_list.append(region_dict)
        if not region_dicts:
            region_dicts = region_dict.copy()
        else:
            for stripe_id, region in region_dict.items():
                if stripe_id not in region_dicts:
                    region_dicts[stripe_id] = region
                else:
                    extend_region(region_dicts[stripe_id], region_dict[stripe_id])

    stripe_envs = {}
    for stripe_id, stripe_region in region_dicts.items():
        stripe_env = os.environ.copy()
        stripe_env["GRASS_REGION"] = gs.region_env(**stripe_region)  # , env=stripe_env)
        stripe_envs[stripe_id] = stripe_env

    queue = ParallelModuleQueue(nprocs)
    for solar_parameter in ["solar_azimuth", "solar_zenith"]:  # mq.items():
        compute_env = stripe_envs["sun_parameters"]
        module_list = []
        for listed_module in module_queues[0][solar_parameter]:
            listed_module.env_ = compute_env
            listed_module.verbose = True
            module_list.append(listed_module)
    queue.put(MultiModule(module_list))

    queue = ParallelModuleQueue(nprocs)
    for band_id, modules in module_queues[0].items():
        if band_id in ["solar_azimuth", "solar_zenith"]:
            continue
        compute_env = stripe_envs[band_id[-2:]]
        module_list = []
        for listed_module in modules:
            listed_module.env_ = compute_env
            listed_module.verbose = True
            module_list.append(listed_module)
    queue.put(MultiModule(module_list))

    if options["register_output"]:
        # Write t.register file if requested
        write_register_file(options["register_output"], import_result)

    return 0


if __name__ == "__main__":
    options, flags = gs.parser()
    # Lazy imports
    try:
        from dateutil.parser import isoparse as parse_timestr
    except ImportError:
        gs.fatal(
            _(
                "Could not import dateutil. Please install it with:\n"
                "'pip install dateutil'!"
            )
        )
    try:
        from osgeo import osr
    except ImportError:
        gs.fatal(
            _("Could not import gdal. Please install it with:\n" "'pip install GDAL'!")
        )

    try:
        from netCDF4 import Dataset
    except ImportError:
        gs.fatal(
            _(
                "Could not import netCDF4. Please install it with:\n"
                "'pip install netcdf4'!"
            )
        )

    try:
        import numpy as np
    except ImportError:
        gs.fatal(
            _(
                "Could not import numpy. Please install it with:\n"
                "'pip install numpy'!"
            )
        )

    atexit.register(cleanup)
    sys.exit(main())
