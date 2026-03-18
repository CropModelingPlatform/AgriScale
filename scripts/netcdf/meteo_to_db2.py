#!/usr/bin/env python

import os
import xarray as xr
import sqlite3
from glob import glob
import numpy as np
import argparse
import sys
import pandas as pd
import traceback
from time import time
from joblib import delayed, Parallel
import re
import tracemalloc
import dask
import dask.array as da
from datetime import datetime
import geopandas as gpd
import rioxarray
import warnings
from xarray import open_zarr
from scipy.ndimage import binary_fill_holes

warnings.filterwarnings("ignore", message=".*vlen-utf8.*")

dask.config.set({"array.slicing.split_large_chunks": False}) 
pd.set_option('display.max_columns', None)

from multiprocessing.pool import ThreadPool

import xarray as xr, os


METEO_COLS = ['idPoint', 'w_date', 'year', 'DOY', 'Nmonth', 'NdayM', 'srad', 'tmax', 'tmin', 'tmoy', 'rain',
          'wind', 'rhum', 'Etppm', 'Tdewmin', 'Tdewmax', 'Surfpress']

import psutil
def clear_memory():
    """Minimal but effective memory cleanup"""
    import gc
    gc.collect()
    
    try:
        xr.backends.file_manager.FILE_CACHE.clear()
    except (NameError, AttributeError):
        pass
    
    try:
        from dask.base import normalize_token
        if hasattr(normalize_token, 'clear_token_cache'):
            normalize_token.clear_token_cache()
    except ImportError:
        pass
                
def log_memory():
    process = psutil.Process(os.getpid())
    print(f"Memory usage: {process.memory_info().rss/1024/1024:.2f} MB", flush=True)

def format_df_meteo(df):

    df['year'] = df['time'].dt.year
    df['Nmonth'] = df['time'].dt.month
    df['NdayM'] = df['time'].dt.day
    df['DOY'] = df['time'].dt.dayofyear
    df['w_date'] = df['time'].dt.strftime('%Y-%m-%d')
    df['idPoint'] = df['lat'].round(4).astype(str) + '_' + df['lon'].round(4).astype(str)
    df = df[METEO_COLS]
    return df

def main():
    #try:
    print("meteo_to_db.py")
    work_dir = '/package'
    inter = '/inter'
    data_dir = '/inputData'
    parser = argparse.ArgumentParser(
        description='load soil data into database')
    parser.add_argument(
        '-i', '--index', help="Specify the index of the sub virtual experience")
    parser.add_argument('-b', '--extract', help="Specify if we need to extract")
    parser.add_argument("--bnd", nargs="+", type=float, help="area bound") 
    parser.add_argument('-m', '--max', help="Specify the max year")
    parser.add_argument('-n', '--min', help="Specify the min year")
    parser.add_argument('--cropmask', help="if crop mask is used")
    parser.add_argument('-o', '--testoption', help="Specify the type of test")
    parser.add_argument('--shp', help="if shapefile is used")
    parser.add_argument('--ncpus', help="number of cpus by task")
    parser.add_argument('--nchunks', help="number of tasks")

    args = parser.parse_args()
    i = args.index
    b = int(args.extract)
    mini = int(args.min)
    maxi = int(args.max)
    cropmask = int(args.cropmask)
    shp = int(args.shp)
    ncpus = int(args.ncpus)
    ntasks = int(args.nchunks)
    n_jobs = ncpus
    bound = args.bnd
    typeoftest = int(args.testoption)

    dask.config.set(scheduler="threads", pool=ThreadPool(n_jobs))


    EXPS_DIR = os.path.join(inter, 'EXPS')
    EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
    DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
    
    if typeoftest!=1:
        ### land
        #ds_mask = xr.open_dataset(glob(os.path.join(data_dir,'land', '*.nc'))[0])
        ds_mask = open_zarr(glob(os.path.join(data_dir,'land', '*.zarr'))[0], consolidated=True)
        if shp == 1:
            shapefile_path = glob(os.path.join(work_dir, 'data', 'shapefile', '*.shp'))[0]
            gdf = gpd.read_file(shapefile_path)
            print(f"we use shapefile {shapefile_path}")
            ds_mask = ds_mask.rio.write_crs("EPSG:4326")
            ds_mask = ds_mask.rio.clip(gdf.geometry, gdf.crs,  all_touched=True, drop=True)
        else:
            print(f"we use bounding box {bound}") 
            ds_mask = ds_mask.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        if cropmask==0: 
            # Si cropmask=0, on veut TOUS les pixels à 1 (pas de masque de culture)
            # Créer un masque rempli de 1 avec les mêmes coordonnées
            ds_mask = xr.Dataset(
                    {"mask": xr.DataArray(
                        data=np.ones((len(ds_mask.lat), len(ds_mask.lon)), dtype=np.float32),
                        dims=["lat", "lon"],
                        coords={"lat": ds_mask.lat, "lon": ds_mask.lon}
                    )},
                    coords=ds_mask.coords
                )
            ds_mask = ds_mask.rio.write_crs("EPSG:4326", inplace=True)
                    
        print(ds_mask)
        print("number of crop pixels", ds_mask["mask"].to_series().dropna().count())
        
            
        ### soil
        SOIL_DIR = os.path.join(data_dir, 'soil')
        ncs = glob(os.path.join(SOIL_DIR, '*.zarr'))
        #ds = xr.open_mfdataset(ncs, cache=False)
        ds_soil = xr.open_mfdataset(
                ncs,
                engine="zarr",
                combine="by_coords",        # fusionne par coordonnées compatibles
                parallel=True               # active Dask
        )
        if shp == 1:
            ds_soil = ds_soil.rio.write_crs("EPSG:4326")
            ds_soil = ds_soil.rio.clip(gdf.geometry, gdf.crs,  all_touched=True,drop=True)
        else: ds_soil = ds_soil.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        

        mask_soil = xr.concat([ds_soil[var].isnull() for var in ds_soil.data_vars], dim='variable').any(dim='variable')
        ds_mask['mask'] = ds_mask['mask'].where(~mask_soil, np.nan)
        
            # sauvegarde si besoin
        #ds_mask.to_netcdf(os.path.join(work_dir, "mask_valid_pixels.nc"))

        ds_mask = ds_mask.reindex({'lat': sorted(ds_mask.lat)})
        df_mask_full = ds_mask.to_dataframe()
        df_mask_full = df_mask_full.reorder_levels(['lat', 'lon'])
        df_mask_full = df_mask_full.sort_index(level='lat')
        df_mask = df_mask_full.dropna(axis=0, how="any")
        df_mask = df_mask.reset_index()

        SLURM_ARRAY_TASK_COUNT = ntasks
        print(len(df_mask), SLURM_ARRAY_TASK_COUNT, "bbbbbbb")
        i = int(i)
        k, m = divmod(len(df_mask), SLURM_ARRAY_TASK_COUNT)
        STEP_START = i * k + min(i, m)
        STEP_END = (i + 1) * k + min(i + 1, m)

        print("STEP_START : " + str(STEP_START))
        print("STEP_END : " + str(STEP_END))
        print("END - START : " + str(STEP_END - STEP_START))
        
        df_mask.loc[~df_mask.index.isin(range(STEP_START, STEP_END)), 'mask'] = None
        df_mask = df_mask.dropna(axis=0, how="any")
        df_mask = df_mask.set_index(['lat', 'lon'])
        da_mask_full = df_mask_full.where(
            df_mask_full.isin(df_mask)).to_xarray()
        ds_mask = ds_mask.where(da_mask_full.mask == 1)
        
        print(ds_mask)
        mask_da = xr.DataArray(ds_mask.mask.to_masked_array(copy=False), dims=('lat', 'lon'))

    METEO_DIR = os.path.join(data_dir, 'meteo')
    parameters = [
        name for name in os.listdir(METEO_DIR)
        if os.path.isdir(os.path.join(METEO_DIR, name))
        ]
    print(f"parameters to process: {parameters}")
    print(f"number of processes is {n_jobs}") 
    print(f"number of processes is {n_jobs}")
    
    def apply_order(year):
        log_memory()
        print(f"Processing year {year}", flush=True)
        year_dfs = []

        for param in parameters:
            ncpath = os.path.join(METEO_DIR, param)
            file_pattern = re.compile(str(year))
            files = [os.path.join(ncpath, f) for f in os.listdir(ncpath) if file_pattern.search(f)]
            if not files:
                print(f"No file for {param} in {year}")
                continue

            with dask.config.set(scheduler="threads", pool=ThreadPool(ncpus), **{'array.slicing.split_large_chunks': False}):
                ds = xr.open_zarr(files[0], consolidated=True).chunk({'time': 365, 'lat': 'auto', 'lon': 'auto'})
                
                if typeoftest==1:
                    if "crs" in ds: ds = ds.drop_vars(["crs"])
                    test = os.path.join(work_dir, 'test', "test.csv")
                    df_test = pd.read_csv(test)
                    xx = xr.DataArray(df_test["lat"].to_list(), dims=['location'])
                    yy = xr.DataArray(df_test["lon"].to_list(), dims=['location'])
                    ds = ds.sel(lat =xx, lon=yy, method = "nearest")
                    year_dfs.append(ds)
                else:
                    if shp == 1:
                        ds = ds.rio.write_crs("EPSG:4326")
                        ds = ds.rio.clip(gdf.geometry, gdf.crs, all_touched=True, drop=True)
                    else:
                        ds = ds.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
                    ds = ds.drop_vars([v for v in ['spatial_ref', 'crs'] if v in ds])
                    ds = ds.reindex({'lat': sorted(ds.lat)})
                    #ds.coords['mask'] = (('lat', 'lon'), ds_mask.mask.to_masked_array(copy=False))

                    ds = ds.where(mask_da == 1, drop=True).persist()
                    year_dfs.append(ds) #df
                ds.close()
                del ds#, df
                clear_memory()

        if not year_dfs:
            return None

        # Fusionner les DataFrames sur lat/lon/time
        combined_ds = xr.merge(year_dfs, compat='override', join='outer')

        # Compute une seule fois ici!
        df_merged = combined_ds.compute().to_dataframe().dropna().reset_index()
        print(df_merged.head(10))
        df_merged.drop(columns=["mask"], errors="ignore", inplace=True)

        del combined_ds
        clear_memory()
        df_merged['time'] = pd.to_datetime(df_merged["time"].dt.date)
        # Postprocessing is moved here!
        df_merged = df_merged.rename(columns={"pr":"rain"})
        df_merged['tmoy'] = df_merged[['tmin', 'tmax']].mean(axis=1)
        df_merged[["lat", "lon"]] = df_merged[["lat", "lon"]].astype(np.float64).round(4)
        target_cols = ['srad', 'tmax', 'tmin', 'tmoy', 'rain', 'wind', 'Tdewmin', 'Tdewmax', 'Surfpress', "rhum", "Etppm"]
        cols_to_cast = [c for c in target_cols if c in df_merged.columns]
        df_merged[cols_to_cast] = df_merged[cols_to_cast].astype(np.float64).round(1)
        for col in target_cols:
            if col not in df_merged.columns:
                df_merged[col] = None
        #df_merged['time'] = pd.to_datetime(df_merged['time'].dt.date)
        df_final = format_df_meteo(df_merged)
        print(f"Processed year {year} with {len(df_final)} records", flush=True)
        return df_final


    with sqlite3.connect(DB_MI) as conn:
        conn.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA synchronous=NORMAL;
                PRAGMA temp_store=MEMORY;
            """)
        cur = conn.cursor()
        cur.executescript("DROP TABLE IF EXISTS RAclimateD;")
        conn.commit()

        for year in range(mini, maxi + 1):
            df_year = apply_order(year)
            if df_year is not None and not df_year.empty:
                df_year.to_sql('RAclimateD', conn, if_exists='append', index=False, method="multi", chunksize=200)
                print(f"✅ Inserted year {year} into RAclimateD")


if __name__ == "__main__":
    main()
