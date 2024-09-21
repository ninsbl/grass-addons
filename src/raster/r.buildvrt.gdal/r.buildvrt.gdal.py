#!/usr/bin/env python3

"""
 MODULE:       r.buildvrt.gdal
 AUTHOR(S):    Stefan Blumentrath
 PURPOSE:      Build GDAL Virtual Rasters (VRT) over GRASS GIS raster maps
 COPYRIGHT:    (C) 2024 by stefan.blumentrath, and the GRASS Development Team

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

"""

# %module
# % description: Build GDAL Virtual Rasters (VRT) over GRASS GIS raster maps
# % keyword: raster
# % keyword: virtual
# % keyword: gdal
# % keyword: patch
# %end

# %option G_OPT_R_INPUTS
# % key: input
# % type: string
# % required: no
# % multiple: yes
# %end

# %option G_OPT_F_INPUT
# % key: file
# % required: no
# %end

# %option G_OPT_R_OUTPUT
# %end

# %flag
# % key: m
# % label: Read data range from metadata
# % description: WARNING: metadata are sometimes approximations with wrong data range
# %end

# %flag
# % key: r
# % label: Create fast link without data range
# % description: WARNING: some modules do not work correctly without known data range
# %end

# %rules
# % required: input,file
# %end


import sys

from pathlib import Path

import grass.script as gs
from grass.pygrass.modules import Module


def get_raster_gdalpath(map_name, check_linked=True, gis_env=None):
    """Get get the path to a raster map that can be opened by GDAL
    Checks for GDAL source of linked raster data and returns those
    if not otherwise requested"""
    if check_linked:
        map_info = gs.find_file(map_name)
        header_path = (
            Path(gis_env["GISDBASE"])
            / gis_env["LOCATION_NAME"]
            / map_info["mapset"]
            / "cell_misc"
            / map_info["name"]
            / "gdal"
        )
        if header_path.exists():
            gdal_path = Path(
                gs.parse_key_val(header_path.read_text().replace(": ", "="))["file"]
            )
            if gdal_path.exists():
                return str(gdal_path)
        gdal_path = Path(
            gs.parse_key_val(
                gs.parse_command("r.info", flags="e", map=map_name)["comments"]
                .replace("\\", "")
                .replace('"', ""),
                vsep=" ",
            )["input"]
        )
        if gdal_path.exists():
            return str(gdal_path)
    gdal_path = Path(gs.find_file(map_name)["file"].replace("/cell/", "/cellhd/"))
    if gdal_path.exists():
        return gdal_path
    gs.fatal(_("Cannot determine GDAL readable path to raster map {}").format(map_name))


def main():
    """run the main workflow"""

    gisenv = gs.gisenv()
    # Create a GDAL directory to place VRTs in
    vrt_dir = Path(gisenv["GISDBASE"]).joinpath(
        gisenv["LOCATION_NAME"], gisenv["MAPSET"], "gdal"
    )

    # Check if GRASS GIS driver is available
    # if not gdal.GetDriverByName("GRASS"):
    #     gs.fatal(
    #         _("GRASS GIS driver missing in GDAL. Please install it.")
    #     )

    if options["input"]:
        inputs = options["input"].split(",")
    else:
        inputs = Path(options["file"]).read_text(encoding="UTF8").strip().split("\n")

    output = options["output"]

    if len(inputs) < 1:
        gs.fatal(_("At least one input map is required".format(inputs[0])))

    inputs = [get_raster_gdalpath(raster_map, gis_env=gisenv) for raster_map in inputs]

    # Get GRASS GIS environment info
    grass_env = dict(gs.gisenv())

    # Create directory for vrt files if needed
    vrt_dir = Path(grass_env["GISDBASE"]).joinpath(
        grass_env["LOCATION_NAME"], grass_env["MAPSET"], "gdal"
    )
    if not vrt_dir.is_dir():
        vrt_dir.mkdir()

    vrt_path = str(vrt_dir / f"{output}.vrt")
    gdal.BuildVRT(vrt_path, inputs)

    # Setup import module object
    Module(
        "r.external",
        quiet=True,
        overwrite=gs.overwrite(),
        flags="roa" if flags["r"] else "moa",
        input=str(vrt_path),
        output=output,
    )


if __name__ == "__main__":
    options, flags = gs.parser()

    # lazy imports
    global gdal
    try:
        from osgeo import gdal
    except ImportError:
        gs.fatal(
            _(
                "Unable to load GDAL Python bindings (requires "
                "package 'python-gdal' or Python library GDAL "
                "to be installed)."
            )
        )

    sys.exit(main())
