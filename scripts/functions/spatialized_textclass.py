#!/usr/bin/env python

import os
import xarray as xr
import sqlite3
from glob import glob
import numpy as np
import argparse
import sys
import traceback
import pandas as pd
pd.set_option('display.max_columns', None)

def createIdSoil(row):
    return str(round(row['lat'],4)) + '_' + str(round(row['lon'],4))

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
        print("spatialized_texture_class.py")

        work_dir = '/package'
        data_dir = '/inputData'
        inter = '/inter'
        parser = argparse.ArgumentParser(description='load soil data into database')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        parser.add_argument('-b', '--extract', help="Specify if we need to extract")
        parser.add_argument("--bnd", nargs="+", type=float, help="area bound") 

        args = parser.parse_args()
        i = args.index
        b = int(args.extract)
        bound = args.bnd
        print(bound[0], bound[1], bound[2], bound[3])
        print(b)

        
        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')

        ds_mask = xr.open_dataset(glob(os.path.join(data_dir, 'land', '*.nc'))[0])
        if b==1: ds_mask = ds_mask.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        print(ds_mask)
        ds_mask = ds_mask.reindex({'lat': sorted(ds_mask.lat)})
        df_mask = ds_mask.to_dataframe().dropna(axis=0, how="any")
        df_mask = df_mask.reorder_levels(['lat', 'lon'])
        df_mask = df_mask.sort_index(level='lat')

        df_mask = df_mask.reset_index()
        df_mask = df_mask.astype(np.float64).round(4)
        df_mask.columns = ['lat', 'lon', 'mask']
        df_mask = df_mask.set_index(['lat', 'lon'])

        SOIL_DIR = os.path.join(data_dir, 'soil')
        ncs = glob(os.path.join(SOIL_DIR, '*texclass*.nc'))[0]
        ds = xr.open_dataset(ncs)
        if b==1: ds = ds.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        ds = ds.reindex({'lat': sorted(ds.lat)})
        ds.coords['mask'] = (('lat', 'lon'), ds_mask.mask.to_masked_array(copy=True))
        # ds = ds.where(ds.mask == 1)
        df = ds.to_dataframe().dropna(axis=0, how="any")
        df = df.reset_index()
        df["IdSoil"] = df.apply(createIdSoil, axis=1)
        df["SoilTextureType"] = df.apply(createsoiltext, axis=1)
        
        df = df.reset_index()
        print(df.head(10))
        conn = sqlite3.connect(DB_MI)
        df_soil = pd.read_sql("select * from soil", conn)

        if 'SoilTextureType_x' in df_soil.columns:
            df_soil.drop('SoilTextureType_x', axis=1, inplace=True)
        if 'SoilTextureType_y' in df_soil.columns:
            df_soil.drop('SoilTextureType_y', axis=1, inplace=True)
        if 'SoilTextureType' in df_soil.columns:
            df_soil.drop('SoilTextureType', axis=1, inplace=True)

        df_soil = df_soil.merge(df[["IdSoil","SoilTextureType"]] ,  how='inner', on='IdSoil')
        print(df_soil.head(10))
        conn.execute('DROP TABLE IF EXISTS soil')
        df_soil.to_sql('soil', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()

    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
