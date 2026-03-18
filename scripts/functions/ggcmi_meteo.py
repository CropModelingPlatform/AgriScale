import os
import re
import xarray as xr
from pathlib import Path

# Folder containing the NetCDF files
data_folder = '/inputData'
outdir  = "/outputData"

# Pattern for file names
pattern = re.compile(r"gswp3-w5e5_obsclim_(.+?)_global_daily_(\d{4})_(\d{4})\.nc$")

for fname in os.listdir(data_folder):
    match = pattern.match(fname)
    if not match:
        continue  # skip files that do not match

    varname, year_start, year_end = match.group(1), int(match.group(2)), int(match.group(3))
    print(f"Processing: {fname}  |  Variable: {varname}  |  Years: {year_start}-{year_end}")

    outdir2 = Path(outdir) / varname
    outdir2.mkdir(exist_ok=True)
    
    fpath = Path(data_folder) / fname
    ds = xr.open_dataset(fpath)

    for year in range(year_start, year_end + 1):
        # Filter data for this year
        year_str = str(year)
        ds_year = ds.sel(time=ds['time'].dt.year == year)

        # Save to NetCDF
        outpath = outdir2 / f"{varname}_{year}.nc"
        print(f"  Saving {outpath} ...")
        ds_year.to_netcdf(outpath)
        ds_year.close()

    ds.close()
