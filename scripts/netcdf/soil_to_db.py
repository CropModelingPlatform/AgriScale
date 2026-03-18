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
import pandas as pd
from xarray import open_zarr
from scipy.ndimage import binary_fill_holes

warnings.filterwarnings("ignore", message=".*vlen-utf8.*")
pd.set_option('display.max_columns', None)

import xarray as xr, os

def createsoiltext(row):
    textclass = {1: "clay",
                 2: "silty clay" ,
                 3: "sandy clay",
                 4: "clay loam",
                 5: "silty clay loam",
                 6: "sandy clay loam",
                 7: "loam",
                 8: "silty loam",
                 9: "sandy loam",
                 10: "silt",
                 11: "loamy sand",
                 12: "sand"}

    return textclass[int(row["Band1"])]

def main():
    try:
        print("soil_to_db.py")
        # work_dir = os.getcwd()
        work_dir = '/package'
        inter = '/inter'
        data_dir = '/inputData'

        #--extract "$doExtract" --bnd "$bound";
        parser = argparse.ArgumentParser(description='load soil data into database')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        parser.add_argument('-b', '--extract', help="Specify if we need to extract")
        parser.add_argument("--bnd", nargs="+", type=float, help="aera bound") 
        parser.add_argument('--soilTexture', help="soil texture")
        parser.add_argument('--cropmask', help="if crop mask is used")
        parser.add_argument('--textclass', help="if soil texture is spatialized")
        parser.add_argument('--shp', help="if shapefile is used")
        parser.add_argument('--nchunks', help="number of tasks")
        parser.add_argument('-o', '--testoption', help="Specify the type of test")
        
        args = parser.parse_args()
        i = args.index
        soilTexture = args.soilTexture
        b = int(args.extract)
        bound = args.bnd
        cropmask = int(args.cropmask)
        textclass = int(args.textclass)
        typeoftest = int(args.testoption)
        print(bound[0], bound[1], bound[2], bound[3])
        print(b)
        shp = int(args.shp)
        ntasks = int(args.nchunks)
        
        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')

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

        if typeoftest !=1 :
            #ds_mask = xr.open_dataset(glob(os.path.join(data_dir, 'land', '*.nc'))[0])
            #ds_mask = open_zarr_cached(glob(os.path.join(data_dir,'land', '*.zarr'))[0], str(args.index), consolidated=True)    
            ds_mask = open_zarr(glob(os.path.join(data_dir,'land', '*.zarr'))[0], consolidated=True)      
            if shp == 1:
                shapefile_path = glob(os.path.join(work_dir,'data', 'shapefile', '*.shp'))[0]
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

        
            if shp == 1:
                ds_soil = ds_soil.rio.write_crs("EPSG:4326")
                ds_soil = ds_soil.rio.clip(gdf.geometry, gdf.crs,  all_touched=True,drop=True)
            else: ds_soil = ds_soil.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        

            mask_soil = xr.concat([ds_soil[var].isnull() for var in ds_soil.data_vars], dim='variable').any(dim='variable')

            ds_mask['mask'] = ds_mask['mask'].where(~mask_soil, np.nan)
            print("number of crop pixels", ds_mask["mask"].to_series().dropna().count())

            ds_mask = ds_mask.reindex({'lat': sorted(ds_mask.lat)})
            df_mask_full = ds_mask.to_dataframe()
            df_mask_full = df_mask_full.reorder_levels(['lat', 'lon'])
            df_mask_full = df_mask_full.sort_index(level='lat')
            df_mask = df_mask_full.dropna(axis=0, how="any")
            df_mask = df_mask.reset_index()

            SLURM_ARRAY_TASK_COUNT = ntasks
            print(len(df_mask), SLURM_ARRAY_TASK_COUNT, "partition soil")
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
            
            print(df_mask.head(10))
            print(ds_soil)
            ds = ds_soil.reindex({'lat': sorted(ds_soil.lat)})
            ds.coords['mask'] = (('lat', 'lon'), ds_mask.mask.to_masked_array(copy=True))
            ds = ds.where(ds.mask == 1)
            df = ds.to_dataframe().dropna(axis=0, how="any")
        else:
            test = os.path.join(work_dir, 'test', "test.csv")
            df_test = pd.read_csv(test)
            yy = xr.DataArray(df_test["lat"].to_list(), dims=['location'])
            xx = xr.DataArray(df_test["lon"].to_list(), dims=['location'])
            df = ds_soil.sel(lat =yy, lon=xx, method = "nearest").to_dataframe().dropna(axis=0, how="any")
        
        print(df.head(5))
        print(len(df))
        df = df.reset_index()
        # lon, lat columns at 4 digits
        df["lon"] = df["lon"].round(4)
        df["lat"] = df["lat"].round(4)
        # create IdSoil column as a concatenation of lat and lon separated by "_"
        df["IdSoil"] = df["lat"].astype(str) + "_" + df["lon"].astype(str)
        # change name of column silt, sand, clay to Silt, Sand, Clay
        #df = df.rename(columns={"silt": "Silt", "sand": "Sand", "clay": "Clay"})
        # round values of columns SoilTotalDepth, SoilRDepth, Wwp, Wfc, bd, OrganicNStock, pH, OrganicC, cf, extp, totp, Sand, Clay, Silt with 1 digit
        df[["SoilTotalDepth", "SoilRDepth", "Wwp", "Wfc", "bd", "OrganicNStock", "pH", "OrganicC", "cf", "extp", "totp", "sand", "clay", "silt"]] = df[["SoilTotalDepth", "SoilRDepth", "Wwp", "Wfc", "bd", "OrganicNStock", "pH", "OrganicC", "cf", "extp", "totp", "sand", "clay", "silt"]].astype(np.float64).round(3)
        # remove columns mask, lat, lon, Band1, mask
        df["totp"] = -99
      
        if textclass == 0:
            # replace SoilTextureType by soilTexture
            df["SoilTextureType"] = soilTexture
        else:
            # create SoilTextureType column
            df["SoilTextureType"] = df.apply(createsoiltext, axis=1)
        
        if typeoftest!=1: 
            df = df.drop(columns=["mask", "lat", "lon", "Band1"])
        else: df = df.drop(columns=["lat", "lon", "Band1"])
        # SoilOption is always simple
        df["SoilOption"] = "simple"
        # Slope is always null
        df["Slope"] = None
        # RunoffType is always 1
        df["RunoffType"] = 1
        # albedo is always 0.3
        df["albedo"] = 0.3
        print(df.head(5))
        with sqlite3.connect(DB_MI) as conn:
            df.to_sql('Soil', conn, if_exists='replace', index=False)
    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
