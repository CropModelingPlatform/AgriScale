import os
import xarray as xr
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from dask.diagnostics import ProgressBar
from dask import config



from math import *
def Ra(lat, J):
    pi = np.pi
    latrad = pi * lat / 180
    Dr = 1 + 0.033 * np.cos(2 * pi * J / 365)
    Declin = 0.409 * np.sin((2 * pi * J / 365) - 1.39)
    SolarAngle = np.tan(latrad) * np.tan(Declin)
    X = -SolarAngle * SolarAngle + 1
    #if X<=0: X = 0.00001
    X = np.where(X <= 0, 0.00001, X)
    SolarAngle = -np.arctan(-SolarAngle / np.sqrt(X)) + 2 * np.arctan(1)
    Ra_ = (SolarAngle * np.sin(latrad) * np.sin(Declin)) + (np.cos(latrad) * np.cos(Declin) * np.sin(SolarAngle))
    Ra_ = 24 * 60 * 0.082 * Dr * Ra_ / pi
    return Ra_


def ET0pm_Tdew(lat, Alt, J, Tn, Tx, Tm, Tdewn, Tdewx, Vm, Rg):
    """
        Calculates ET0 according to FAO Penman-Monteith Equation (Bull.FAO#56)
        altitude Alt in m,
        J in number of the day in the year
        Tn, Tx and Tm respectively minimal,maximal and mean daily temperature in degrees C, Tx, Tm,
        Tdewn, Tdewx, respectively minimal and maximal dewpoint temperature in degrees C
        Vm,average wind distance perr day  in km,
        Rg global radiation in MJ/m2/day
        All variables assumed measured at 2m above soil
    """
    sigma = 0.000000004903
    if (lat is None or Alt is None or J is None or Tn is None or Tx is None or  Tm is None or Tdewn is None or Tdewx is None or  Vm is None or  Rg is None):
        return None
    else:
        gamma = 101.3 * ((293 - 0.0065 * (Alt)) / 293) ** 5.26
        gamma = 0.000665 * gamma
        E0SatTn = 0.6108 * np.exp(17.27 * Tn / (Tn + 237.3))
        E0SatTx = 0.6108 * np.exp(17.27 * Tx / (Tx + 237.3))
        SlopeSat = 4098 * (0.6108 * np.exp(17.27 * Tm / (Tm + 237.3))) / ((Tm + 237.3) ** 2)
        Ea = 0.5 * 0.6108 * (np.exp(17.27 * Tdewx / (Tdewx + 237.3)) + np.exp(17.27 * Tdewn / (Tdewn + 237.3)))
        VPD = ((E0SatTn + E0SatTx) / 2) - Ea
        adv = gamma * 900 * Vm * VPD / (Tm + 273)
        Rso = (0.00002 * Alt + 0.75) * Ra(lat, J)
        Rns = (1 - 0.23) * Rg
        Rnl = Rg / Rso
        Rnl = np.where(Rnl > 1, 1, Rnl)
        Rnl = (Rnl * 1.35 - 0.35) * (-0.14 * np.sqrt(Ea) + 0.34)
        Tn = Tn + 273.16
        Tx = Tx + 273.16
        Rnl = sigma * (Tn ** 4 + Tx ** 4) * Rnl / 2
        #radiation balance assuming soil heat flux is 0 at a day time step
        Rad = 0.408 * SlopeSat * (Rns - Rnl)
        #ajout des deux termes
        return (Rad + adv) / (SlopeSat + gamma * (0.34 * Vm + 1))



data_dir = '/inputData'
outdir  = "/output"

# Path definitions
DEM_FOLDER = os.path.join(data_dir,"dem")
TEMPMIN_FOLDER = os.path.join(data_dir,"meteo", "Temperature-Air-2m-Min-24h")
TEMPMAX_FOLDER = os.path.join(data_dir,"meteo", "Temperature-Air-2m-Max-24h")
TEMPAVERAGE_FOLDER = os.path.join(data_dir,"meteo", "Temperature-Air-2m-Mean-24h")
WIND_FOLDER = os.path.join(data_dir,"meteo", "Wind-Speed-10m-Mean")
TDEWMIN_FOLDER = os.path.join(data_dir,"meteo", "2m-dewpoint-temperature-min")
TDEWMAX_FOLDER = os.path.join(data_dir,"meteo", "2m-dewpoint-temperature-max")
SRD_FOLDER = os.path.join(data_dir,"meteo", "Solar-Radiation-Flux")

ETP_OUTPUT_FOLDER = os.path.join(outdir, "etp")

# Ensure the output folder exists
os.makedirs(ETP_OUTPUT_FOLDER, exist_ok=True)

# Load elevation data
elevation_file = os.path.join(DEM_FOLDER, "MERIT_DEM_5km_final.nc")
elevation_data = xr.open_dataset(elevation_file).Band1

# Function to process a single year's data
for year in range(1981, 2023):
    print(f"Processing year {year}...")

    tempmin_file = os.path.join(TEMPMIN_FOLDER, f"Temperature-Air-2m-Min-24h_{year}.nc")
    tempmax_file = os.path.join(TEMPMAX_FOLDER, f"Temperature-Air-2m-Max-24h_{year}.nc")
    tempavg_file = os.path.join(TEMPAVERAGE_FOLDER, f"Temperature-Air-2m-Mean-24h_{year}.nc")
    wind_file = os.path.join(WIND_FOLDER, f"Wind-Speed-10m-Mean_{year}.nc")
    tdewmin_file = os.path.join(TDEWMIN_FOLDER, f"2m-dewpoint-temperature-min_{year}.nc")
    tdewmax_file = os.path.join(TDEWMAX_FOLDER, f"2m-dewpoint-temperature-max_{year}.nc")
    srd_file = os.path.join(SRD_FOLDER, f"Solar-Radiation-Flux_{year}.nc")


    # Load data
    tempmin_data = xr.open_dataset(tempmin_file).tmin
    tempmax_data = xr.open_dataset(tempmax_file).tmax
    tempavg_data = xr.open_dataset(tempavg_file).tmoy
    wind_data = xr.open_dataset(wind_file).wind
    tdewmin_data = xr.open_dataset(tdewmin_file).Tdewmin
    tdewmax_data = xr.open_dataset(tdewmax_file).Tdewmax
    srd_data = xr.open_dataset(srd_file).srad


    # Ensure data alignment
    aligned_data = xr.align(
        tempmin_data, tempmax_data, tempavg_data, tdewmin_data, tdewmax_data, wind_data, srd_data, join="override"
    )

    # Calculate ETP
    #etp_values = calculate_etp.ET0pm_Tdew(x["latitude"], x["altitude"], x["DOY"], x["tmin"], x["tmax"], x["tmoy"], x["Tdewmin"], x["Tdewmax"], x["wind"], x["srad"]), axis=1)

    # Add a day_of_year variable
    day_of_year = aligned_data[0].time.dt.dayofyear

    print(aligned_data[0].lat.dims, elevation_data.dims, day_of_year.dims, aligned_data[0].dims)

    lat_b, Alt_b, J_b = xr.broadcast(aligned_data[0], elevation_data, day_of_year)
    print(lat_b.dims, Alt_b.dims, J_b.dims)

    # Use Dask for parallel processing
    with config.set(scheduler="processes"):  # Enable multi-core processing
        etp_values = xr.apply_ufunc(
            ET0pm_Tdew,
            lat_b,
            Alt_b,
            J_b,
            aligned_data[0],  # temp_min
            aligned_data[1],  # temp_max
            aligned_data[2],  # temp_avg
            aligned_data[3],  # tdew_min
            aligned_data[4],  # tdew_max
            aligned_data[5],  # wind
            aligned_data[6],  # srad
            input_core_dims=[ ['time', 'lat', 'lon']]*10,
            output_core_dims=[["time", "lat", "lon"]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[np.float32],
        )

        # Create a new Dataset for ETP
        print("xr.ufun is done")
        etp_dataset = xr.Dataset(
            {
                "etp": (("time", "lat", "lon"), etp_values.data)
            },
            coords={
                "time": tempmin_data.time,
                "lat": tempmin_data.lat,
                "lon": tempmin_data.lon
            },

            attrs={
                "missing_value": np.nan,  # Set missing value for ETP similar to temp
                "_FillValue": np.nan,     # Set fill value
            }
        )

        # Save the result to a NetCDF file
        etp_output_file = os.path.join(ETP_OUTPUT_FOLDER, f"etp_{year}.nc")
        with ProgressBar():
            etp_dataset.to_netcdf(etp_output_file,             
				        encoding={
                                             "etp": {
                    				  "dtype": "float32",  # Ensure float32 dtype for storage
                                                  "chunksizes": (1, 1243, 1380),  # Match chunk sizes from temp
                                                  "_FillValue": np.nan,  # Set _FillValue for the etp variable,
 						  "zlib": True,
                                                   "complevel": 9
                                                    }
                                                 }
				)

        print(f"ETP for year {year} saved to {etp_output_file}")