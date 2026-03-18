#!/usr/bin/env python

import os
import xarray as xr
import sqlite3
from glob import glob
import numpy as np
import argparse
import sys
import traceback
import geopandas as gpd
import rioxarray
import warnings
from xarray import open_zarr
from scipy.ndimage import binary_fill_holes
import pandas as pd
warnings.filterwarnings("ignore", message=".*vlen-utf8.*")

import xarray as xr, os


def main():
    try:
        print("dem_to_db.py")
        work_dir = '/package'
        inter = '/inter'
        data_dir = '/inputData'        
        parser = argparse.ArgumentParser(description='load soil data into database')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        parser.add_argument('-b', '--extract', help="Specify if we need to extract")
        parser.add_argument("--bnd", nargs="+", type=float, help="area bound") 
        parser.add_argument('--cropmask', help="if crop mask is used")
        parser.add_argument('--shp', help="if shapefile is used")
        parser.add_argument('--nchunks', help="number of tasks")
        parser.add_argument('-o', '--testoption', help="Specify the type of test")


        args = parser.parse_args()
        i = args.index
        b = int(args.extract)
        cropmask = int(args.cropmask)
        bound = args.bnd
        typeoftest = int(args.testoption)
        print(bound[0], bound[1], bound[2], bound[3])
        print(b)
        shp = int(args.shp)
        ntasks = int(args.nchunks)

        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
        
        ds_dem = open_zarr(glob(os.path.join(data_dir,'dem', '*.zarr'))[0], consolidated=True)
        if typeoftest !=1 :
            ds_mask = open_zarr(glob(os.path.join(data_dir,'land', '*.zarr'))[0],  consolidated=True)
            ds_mask = ds_mask.rio.write_crs("EPSG:4326", inplace=True) 

            if shp == 1:
                shapefile_path = glob(os.path.join(work_dir,'data','shapefile', '*.shp'))[0]
                gdf = gpd.read_file(shapefile_path)
                ds_mask = ds_mask.rio.write_crs("EPSG:4326")
                ds_mask = ds_mask.rio.clip(gdf.geometry, gdf.crs, all_touched=True, drop=True)

            else: ds_mask = ds_mask.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
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
            ds_soil = xr.open_mfdataset(
                    ncs,
                    engine="zarr",
                    combine="by_coords",        # fusionne par coordonnées compatibles
                    parallel=True               # active Dask
            )
            if shp == 1:
                ds_soil = ds_soil.rio.write_crs("EPSG:4326")
                ds_soil = ds_soil.rio.write_crs("EPSG:4326")
                ds_soil = ds_soil.rio.clip(gdf.geometry, gdf.crs, all_touched=True, drop=True)
            else: ds_soil = ds_soil.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
            
            mask_soil = xr.concat([ds_soil[var].isnull() for var in ds_soil.data_vars], dim='variable').any(dim='variable')
            ds_mask['mask'] = ds_mask['mask'].where(~mask_soil, np.nan)

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
            
            if shp == 1: 
                ds_dem = ds_dem.rio.write_crs("EPSG:4326")
                ds_dem = ds_dem.rio.clip(gdf.geometry, gdf.crs, all_touched=True, drop=True)
            else: ds_dem = ds_dem.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
            print(ds_dem)
            ds_dem = ds_dem.reindex({'lat': sorted(ds_dem.lat)})
            ds_dem.coords['mask'] = (('lat', 'lon'), ds_mask.mask.to_masked_array(copy=True))
            ds_dem = ds_dem.where(ds_dem.mask == 1, drop=True)
        
        else:
            test = os.path.join(work_dir, 'test', "test.csv")
            df_test = pd.read_csv(test)
            yy = xr.DataArray(df_test["lat"].to_list(), dims=['location'])
            xx = xr.DataArray(df_test["lon"].to_list(), dims=['location'])
            ds_dem = ds_dem.sel(lat =yy, lon=xx, method = "nearest")    
        
        print(ds_dem)
        ds_dem = ds_dem.drop_vars(["crs"], errors='ignore')
        df = ds_dem.to_dataframe().dropna(axis=0, how="any")
        df = df.reset_index()
        if typeoftest!=1: df.drop(['mask'], axis=1, inplace=True)
        print(df.head(5))
        df = df.rename(columns={'Band1':'dem_average'})
        df = df.astype(np.float64).round(4)

        with sqlite3.connect(DB_MI) as conn:
            df.to_sql('DemTemp', conn, if_exists='replace', index=False)

        sql_as_string = ''
        with open(os.path.join(work_dir, 'scripts', 'db', 'init_coordinates.sql')) as f:
            sql_as_string = f.read()

        with sqlite3.connect(DB_MI) as conn:
            cur = conn.cursor()
            cur.executescript(sql_as_string)
            conn.commit()
            cur.executescript("DROP TABLE DemTemp;")
            conn.commit()
    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()