import xarray as xr
import numpy as np
from glob import glob
import os
import argparse
import geopandas as gpd
import re
from dask.diagnostics import ProgressBar

def main():
    input_dir = '/inputData'
    output_dir = '/outputData'
    parser = argparse.ArgumentParser(description='Calculate cumulative thermal time from temperature data')
    parser.add_argument("--tbase", type=float, default=8.0, help="Base temperature in degrees Celsius") 
    parser.add_argument('-m', '--maxi', help="Specify the max year")
    parser.add_argument('-n', '--mini', help="Specify the min year")
    args = parser.parse_args()
    TBASE = float(args.tbase)  # Base temperature in degrees Celsius
    print(f"Using base temperature: {TBASE} °C", flush=True)
    mini = int(args.mini)
    maxi = int(args.maxi)

    
    #load planting and harvest maps
    ph = xr.open_dataset(glob(os.path.join(input_dir, "planting_harvest", "*.nc4"))[0], decode_timedelta=False)  # vars: planting, harvest

    METEO_DIR = os.path.join(input_dir, 'meteo')
    parameters = [ "Temperature-Air-2m-Max-24h", "Temperature-Air-2m-Min-24h"] 

    # Loop over years and stack results
    years = list(range(mini, maxi + 1))
    for year in years:
        file_pattern_1 = re.compile(str(year))
        file_pattern_2 = re.compile(str(year+1))
        tmax_files = [os.path.join(os.path.join(METEO_DIR, parameters[0]), f) for f in os.listdir(os.path.join(METEO_DIR, parameters[0])) if file_pattern_1.search(f) or file_pattern_2.search(f)]
        tmin_files = [os.path.join(os.path.join(METEO_DIR, parameters[1]), f) for f in os.listdir(os.path.join(METEO_DIR, parameters[1])) if file_pattern_1.search(f) or file_pattern_2.search(f)]
        print(f"Processing year {year}")
        print(f"Processing year {year} with files: {tmax_files} {tmin_files}")
        tmax = xr.open_mfdataset(tmax_files, combine="by_coords", chunks={})  # Load with dask for parallel processing
        tmin = xr.open_mfdataset(tmin_files, combine="by_coords", chunks={})  # Load with dask for parallel processing
        tmax = tmax.drop_vars([v for v in ['spatial_ref', 'crs'] if v in tmax])
        tmin = tmin.drop_vars([v for v in ['spatial_ref', 'crs'] if v in tmin])
        print("tmax tmin dims", tmax.dims, tmin.dims)
        print("tmax tmin coords", tmax.coords, tmin.coords)
        tmax = tmax.chunk({'time': -1})
        tmin = tmin.chunk({'time': -1})
        ctt = compute_thermal_time_year(tmax, tmin,TBASE, ph, year)
        print(ctt.head(10))
        print("what up")
        print("tmax", tmax)
        print("tmin", tmin)
        print("ctt", ctt)

        print("xr.ufun is done")
        gdd_dataset = xr.Dataset(
            {
                "gdd": (("lat", "lon"), ctt.data)
            },
            coords={
                "lat": ctt.lat,
                "lon": ctt.lon
            },

            attrs={
                "missing_value": np.nan,  # Set missing value for GDD similar to temp
                "_FillValue": np.nan,     # Set fill value
            }
        )
        
        print(f"Year {year} GDD calculated with shape: {gdd_dataset.gdd.shape}", flush=True)
        
        # Save the result to a NetCDF file
        gdd_output_file = os.path.join(output_dir, f"gdd_{year}.nc")
        with ProgressBar():
            gdd_dataset.to_netcdf(gdd_output_file,             
				        encoding={
                                             "gdd": {
                    				  "dtype": "float32",  # Ensure float32 dtype for storage
                                                  "_FillValue": np.nan,  # Set _FillValue for the gdd variable,
 						  "zlib": True,
                                                   "complevel": 9
                                                    }
                                                 }
				)

        print(f"GDD for year {year} saved to {gdd_output_file}")


def mask_time_range(da, doy, planting, harvest, years, ref_year):
    """
    Crée un masque temporel prenant en compte les cultures intra-annuelles
    et celles qui chevauchent deux années.
    """
    cond_same_year = (harvest >= planting) & (doy >= planting) & (doy <= harvest) & (years==ref_year)
    #cond_cross_year = (harvest < planting) & ((doy >= planting) | (doy <= harvest))

    cond_cross_year = (
        (harvest < planting) & (
            ((years == ref_year) & (doy >= planting)) |   # fin annee N
            ((years == ref_year + 1) & (doy <= harvest))  # debut annee N+1
        )
    )

    mask = cond_same_year | cond_cross_year
    return np.where(mask, da, np.nan)

# Function to compute thermal time for a year
def compute_thermal_time_year(tmax, tmin, TBASE, ph, year):

    tavg = (tmax["tasmax"] + tmin["tasmin"]) / 2 - 273.15  # Convert from Kelvin to Celsius
    tavg = tavg.where(tavg > TBASE, 0)
    tavg = tavg.chunk({'time': -1})
    years = tavg['time'].dt.year
    print("avgggggggggg")
    print(tavg, years)
    doy = tavg['time'].dt.dayofyear

    planting = ph['planting_day']
    harvest = ph['maturity_day']
    thermal_masked = xr.apply_ufunc(
      mask_time_range,
      tavg,
      doy,
      planting,
      harvest,
      years,
      kwargs={"ref_year": int(year)},
      input_core_dims=[['time'], ['time'], [], [], ['time']],
      output_core_dims=[['time']],
      vectorize=True,
      dask='parallelized',
      dask_gufunc_kwargs={'allow_rechunk': True},
      output_dtypes=[np.float32]
    )    
    thermal_masked = thermal_masked.where((years == year) | (years == year + 1))
    thermal_time = (thermal_masked - TBASE).clip(min=0)
    cumulative_tt = thermal_time.sum(dim='time')
    return cumulative_tt


if __name__ == "__main__":
    main()
    print("Thermal time calculation completed successfully.", flush=True)
# This script calculates cumulative thermal time from temperature data.
# It processes Tmax and Tmin data for specified years, applies a mask based on planting and harvest dates,
# and computes the thermal time using a base temperature. The results are saved to a NetCDF file.