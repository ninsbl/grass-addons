#!/usr/bin/env python

############################################################################
#
# MODULE:      i.jaxa.import
# AUTHOR(S):   Stefan Blumentrath
# PURPOSE:     Import Satellite Data Products from the JAXA data portal
# COPYRIGHT:   (C) 2021 by Stefan Blumentrath, and the GRASS development team
#
#              This program is free software under the GNU General Public
#              License (>=v2). Read the file COPYING that comes with GRASS
#              for details.
#
#############################################################################

#%module
#% description: Import Satellite Data Products from the JAXA data portal
#% keyword: imagery
#% keyword: satellite
#% keyword: JAXA
#% keyword: GCOM
#% keyword: import
#%end

#%option G_OPT_M_DIR
#% key: input
#% description: Name for input directory where downloaded JAXA data is stored
#% required: yes
#% guisection: Input
#%end

#%option
#% key: predefined_product
#% type: string
#% description: Pre-defined JAXA products to download
#% required: no
#% options: GCOM_C_LST_L2_250m_daily, GCOM_C_EVI_L3_1_24_deg_daily
#% guisection: Input
#%end

#%option
#% key: product
#% type: string
#% description: Data product to search for
#% required: no
#% guisection: Product
#%end

#%option
#% key: version
#% type: string
#% description: Version of the data product to search for
#% required: no
#% options: 1, 2, 3
#% answer: 2
#% guisection: Product
#%end

#%option
#% key: time_unit
#% type: string
#% description: Time unit of the dataset to search for
#% required: no
#% options: 1_day,  8_days, 1_month
#% guisection: Product
#%end

#%option
#% key: projection
#% type: string
#% description: Projection of the dataset to search for
#% required: no
#% options: EQA_1_dimension, EQA, EQR, PS_N, PS_S, Tile
#% guisection: Product
#%end

#%option
#% key: resolution
#% type: string
#% description: Resolution of the dataset to search for
#% required: no
#% options: 250m, 1000m, 1_24deg, 1_12deg
#% guisection: Product
#%end

#%option
#% key: tiles
#% type: string
#% description: Tiles to download (ignored when not relevant)
#% required: no
#% guisection: Product
#%end

#%option
#% key: start
#% type: string
#% description: Start date ('YYYY-MM-DD')
#% guisection: Optional
#%end

#%option
#% key: end
#% type: string
#% description: End date ('YYYY-MM-DD')
#% guisection: Optional
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
#% description: List filtered products and exit
#% guisection: Optional
#%end

#%rules
#% exclusive: -l, output
#%end


from calendar import monthrange
from datetime import datetime
import ftplib
from itertools import chain
from multiprocessing import Pool
import os
from pathlib import Path
import sys

import grass.script as gscript

path = gscript.utils.get_lib_path(modname="jaxalib", libname="config")
if path is None:
    gscript.fatal("Not able to find the jaxalib library directory.")
sys.path.append(path)

import config as jl

# Use settings solution from i.sentinel.download

# Move shared objects (e.g. product descriptions) into a jaxalib
# Download scenes based on user requests for predefined product or: sensor, product, level, time, time_unit, projection, resolution (not all combinations are valid)...
# Download only if newer
# Download in parallel (lower priority as authentication seems slow, maybe chunks of URLs)
# List content of the server (including URLs)

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


def get_mtime(file):
    return datetime.utcfromtimestamp(os.path.getmtime(file))


def list_content(user, user_options):
    """list"""
    # Add a flag for listing recursively:
    # if not all components are defined (e.g. predefined product)
    ## missing components will be treated as wild card (and all sub directories are listed) otherwise
    # stop at the first missing hirarchy level and print content
    # base_component (directory where datasets are stored)
    # Open the FTP connection
    with ftplib.FTP(jl.ftp_url) as ftp:
        ftp.login(user, "anonymous")
        ftp.encoding = "utf-8"
        # Initialize variables
        url_base = ""
        # Walk hierachy levels
        for idx, level in enumerate(jl.url_components):
            # Stop if level is not provided
            if level not in user_options or not user_options[level]:
                break
            current_level = level
            next_level = str(user_options[level])
            try:
                # change the current working directory to the respecive level
                ftp.cwd(next_level)
            except ValueError:
                gscript.fatal(
                    _(
                        "Level {level} not found in {url}".format(
                            level=level, url=url_base
                        )
                    )
                )
            # Add level to base URL
            url_base += f"/{next_level}"
        # List content of directory
        content = list(ftp.nlst())
        # Return if product definition is incomplete
        if current_level != "version":
            return content, url_base, current_level, None, None

        # Filter by time
        available_years = sorted(map(int, [y for y in content if y.isdigit()]))
        # Get / set first month with data
        first_year = available_years[0]
        start_time = user_options["start"]
        if not start_time or first_year >= start_time.year:
            ftp.cwd(str(first_year))
            first_month = int(sorted([m for m in ftp.nlst() if m.isdigit()])[0])
            first_day = 1
            ftp.cwd("..")
            if start_time and (
                first_year == start_time.year and first_month <= start_time.month
            ):
                first_month = start_time.month
                first_day = start_time.day
        else:
            first_year = start_time.year
            first_month = start_time.month
            first_day = start_time.day
        if not start_time or start_time != datetime(first_year, first_month, first_day):
            gscript.info(
                _(
                    "First available time periode with data is {year}/{month}".format(
                        year=first_year, month=first_month
                    )
                )
            )
        start_time = datetime(first_year, first_month, first_day)
        # Get / set last month with data
        last_year = available_years[-1]
        end_time = user_options["end"]
        if not end_time or last_year <= end_time.year:
            ftp.cwd(str(last_year))
            last_month = int(sorted([m for m in ftp.nlst() if m.isdigit()])[-1])
            last_day = monthrange(last_year, last_month)[1]
            ftp.cwd("..")
        else:
            last_year = end_time.year
            last_month = end_time.month
            last_day = end_time.day
        if not end_time or end_time != datetime(last_year, last_month, last_day):
            gscript.info(
                _(
                    "Last available time periode with data is {year}/{month}".format(
                        year=last_year, month=last_month
                    )
                )
            )
            end_time = datetime(last_year, last_month, last_day)
    return content, url_base, current_level, start_time, end_time


def get_time_range(start, end):
    """Get years and months as string within time interval"""
    if start.year == end.year and start.month == end.month:
        return ["{0}/{1:02d}".format(end.year, end.month)]
    get_month = lambda dt: dt.month + 12 * dt.year
    return [
        "{0}/{1:02d}".format(*divmod(m, 12))
        for m in range(get_month(start), get_month(end))
    ]


def get_content(user, base_url, time_slot, user_options, regex, print_files):
    """List relevant content of the G-portal"""
    # Download by directory
    # Add a flag to filter by modification time, not sensing time
    # Open the FTP connection
    with ftplib.FTP(jl.ftp_url) as ftp:
        ftp.login(user, "anonymous")
        ftp.encoding = "utf-8"
        try:
            ftp.cwd(f"{base_url}/{time_slot}")
        except Exception:
            gscript.warning(_("Cannot open URL: {}".format(f"{base_url}/{time_slot}")))
            return
        content = ftp.mlsd()
        content_dict = {"dirs": [], "files": {}}
        content_dict["files"]["."] = []
        for file in content:
            if file[1]["type"] == "dir":
                content_dict["dirs"].append(file[0])
            elif file[1]["type"] == "file":
                if regex["area_global" if file[0][19] == "_" else "scene"].match(
                    file[0]
                ):
                    content_dict["files"]["."].append(
                        (
                            file[0],
                            datetime.strptime(file[1]["modify"], "%Y%m%d%H%M%S"),
                            float(file[1]["size"]) / 1024.0 / 1024.0
                            if "size" in file[1]
                            else None,
                        )
                    )
        # List sub-directories
        if content_dict["dirs"]:
            for sub_dir in sorted(content_dict["dirs"]):
                try:
                    if (
                        datetime.strptime(f"{time_slot}/{sub_dir}", "%Y/%m/%d")
                        > user_options["end"]
                    ):
                        continue
                except Exception:
                    pass
                try:
                    ftp.cwd(sub_dir)
                except Exception:
                    gscript.warning(
                        _(
                            "Cannot open URL: {}".format(
                                f"{base_url}/{time_slot}/{sub_dir}"
                            )
                        )
                    )
                    continue
                content_dict["files"][sub_dir] = []
                content = ftp.mlsd()
                for file in content:
                    if file[1]["type"] == "file" and regex[
                        "area_global" if file[0][19] == "_" else "scene"
                    ].match(file[0]):
                        content_dict["files"][sub_dir].append(
                            (
                                file[0],
                                datetime.strptime(file[1]["modify"], "%Y%m%d%H%M%S"),
                                float(file[1]["size"]) / 1024.0 / 1024.0
                                if "size" in file[1]
                                else None,
                            )
                        )
                ftp.cwd("..")
        # Return if download is not requested
        if print_files:
            return content_dict["files"]
        for folder, file_list in content_dict["files"].items():
            for file in file_list:
                local_file = user_options["output"].joinpath(file[0])
                rest = {"rest": None}
                # Check if file exists locally
                if local_file.exists():
                    # Check local file size
                    local_size = local_file.stat().st_size
                    # Check if local file is complete
                    if local_size / 1024.0 / 1024.0 < file[2]:
                        # Check if local file is newer than remote
                        if file[1] <= get_mtime(local_file):
                            gscript.info(
                                _(
                                    "{file_name} is incomplete, resuming download".format(
                                        file_name=file[0]
                                    )
                                )
                            )
                            rest = {"rest": str(local_size)}
                        else:
                            gscript.info(
                                _(
                                    "{file_name} is newer in the data portal, downloading the updated version".format(
                                        file_name=file[0]
                                    )
                                )
                            )
                    elif file[1] < get_mtime(local_file):
                        gscript.info(
                            _(
                                "{file_name} is newer on disk, skipping download".format(
                                    file_name=file[0]
                                )
                            )
                        )
                        continue
                # Download file
                try:
                    with open(local_file, "wb") as f:
                        ftp.retrbinary(
                            "RETR {}/{}".format(folder, file[0]), f.write, **rest
                        )
                except Exception:
                    gscript.warning(_("Could not download {}".format(file[0])))
    return 0


def main():
    """Do the main work"""

    # Check date inputs
    # Settings
    if options["settings"] == "-":
        user = input(_("Insert username: "))
    else:
        try:
            with open(options["settings"], "r") as fd:
                lines = list(filter(None, (line.rstrip() for line in fd)))
                if len(lines) < 1:
                    gscript.fatal(_("Invalid settings file"))
                user = lines[0].rstrip()
        except IOError as e:
            gscript.fatal(_("Unable to open settings file: {}").format(e))

    if options["output"]:
        out_path = Path(options["output"])
        if not out_path.exists():
            try:
                out_path.mkdir(parents=True)
            except Exception:
                gscript.fatal(
                    _("Output directory does not exist and cannot be created.")
                )
        options["output"] = out_path

    # Temporal filter
    date_msg = "Cannot parse given {} date. Please check the input format."
    for date_ref in ["start", "end"]:
        if options[date_ref]:
            try:
                options[date_ref] = datetime.strptime(options[date_ref], "%Y-%m-%d")
            except Exception:
                gscript.fatal(_(date_msg.format(date_ref)))

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
    content, url_base, last_level, start, end = list_content(user, options)

    # Get relevant / requested time range
    if start and end:
        time_range = get_time_range(start, end)
    else:
        result = content if isinstance(content, list) else [[content]]

    if last_level == "version":
        if int(options["nprocs"]) > 1:
            pool = Pool(int(options["nprocs"]))
            result = pool.starmap(
                get_content,
                [
                    (user, url_base, time_slot, options, regex, flags["l"])
                    for time_slot in time_range
                ],
            )
        else:
            result = [
                get_content(user, url_base, time_slot, options, regex, flags["l"])
                for time_slot in time_range
            ]
        if flags["l"]:
            result = chain.from_iterable(
                [v for r in result if r is not None for v in r.values()]
            )
            result = [
                "|".join(map(str, c)) if isinstance(c, tuple) else str(c)
                for c in result
            ]
            print("\n".join(sorted(result)))

    else:
        print(
            "\n".join(
                sorted(["|".join(map(str, c)) if c is list else c for c in result])
            )
        )


if __name__ == "__main__":
    options, flags = gscript.parser()
    main()
