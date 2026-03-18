#!/usr/bin/env python

import os
import sqlite3
import argparse
import sys
import traceback
import pandas as pd
import xarray as xr
from glob import glob
import json
import geopandas as gpd
import numpy as np
import rioxarray
from scipy.ndimage import binary_fill_holes

#python ${DATAMILL_WORK}/scripts/functions/spatialized_cropMangt.py --index $i --svariety "$s_variety" --sfert "$s_fert" --sirr "$s_irr" --ssowing "$s_sowing" --sdensity "$s_density" --variety_dict "$variety_dict" --sowingDates "${SW[@]}";

def main():
    try:
        print("spatialized_cropMangt.py")
        work_dir = '/package'
        inter = '/inter'
        data_dir = '/inputData' 
        parser = argparse.ArgumentParser(description='load')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        parser.add_argument(
        "--variety_dict", 
        type=str, 
        required=True, 
        help="JSON string representing a dictionary of varieties"
        )
        parser.add_argument("--svariety", help="Specify the variety option")
        parser.add_argument("--sfert", help="Specify the fertilization option")
        parser.add_argument("--sirr", help="Specify the irrigation option")
        parser.add_argument("--ssowing", help="Specify the sowing option")
        parser.add_argument("--sdensity", help="Specify the density option")
        parser.add_argument('-d', '--sowingDates',type=int, nargs="+", help="Specify the list of sowing dates")
        parser.add_argument("--bnd", nargs="+", type=float, help="aera bound")
        parser.add_argument('--sowingoption', help="Specify the sowing option")
        parser.add_argument("--cropvariety", nargs="*", help="Crop variety")
        parser.add_argument("--ferti", nargs="*", help="fertilizer option") 
        parser.add_argument("--shp", help="Specify if the shapefile path is used")
        parser.add_argument('--cropmask', help="if crop mask is used")
        parser.add_argument('--nchunks', help="number of tasks")

        scratch = 0
        args = parser.parse_args()
        cropmask = int(args.cropmask)
        ntasks = int(args.nchunks)
        
            # Convert the JSON string to a Python dictionary
        try:
            variety_dict = json.loads(args.variety_dict)
        except json.JSONDecodeError:
            print("Error: Invalid JSON format in --variety_dict")
            return
        
        # Use the dictionary
        print("Parsed variety dictionary:", variety_dict)
        for key, value in variety_dict.items():
            print(f"{key}: {value}")
            print(f"{type(key)}: {type(value)}")
        
        i = int(args.index)
        s_variety = int(args.svariety)
        s_fert = int(args.sfert)
        s_irrig = int(args.sirr)
        s_sowing = int(args.ssowing)
        s_density = int(args.sdensity)
        shapefile = int(args.shp)

        sd = args.sowingoption
        bound = args.bnd
        print(bound[0], bound[1], bound[2], bound[3])  
        
        variety = args.cropvariety
        fertioption = args.ferti
        
        nc_variety_file = None
        nc_fert_file = None
        nc_irrig_file = None
        nc_sowing_file = None
        nc_density_file = None
        
        sw = args.sowingDates
        print(sw)

        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')

	    # Reset CropManagement table
        dbori = os.path.join(work_dir, 'db', 'MasterInput.db')

        conn_sq1 = sqlite3.connect(dbori)

        df_mangt= pd.read_sql('SELECT * FROM CropManagement', conn_sq1) 
        conn_sq1.close() 

        ds_mask = xr.open_zarr(glob(os.path.join(data_dir,'land', '*.zarr'))[0], consolidated=True)
        ds_mask = ds_mask.rio.write_crs("EPSG:4326", inplace=True) 

        if shapefile == 1:
            shapefile_path = glob(os.path.join(work_dir,'data','shapefile', '*.shp'))[0]
            gdf = gpd.read_file(shapefile_path)
            ds_mask = ds_mask.rio.write_crs("EPSG:4326")
            ds_mask = ds_mask.rio.clip(gdf.geometry, gdf.crs, drop=True)

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
            
            # save the mask file before soil
            ds_mask.to_netcdf(os.path.join(EXP_DIR, 'mask_before_soil.nc'))
        SOIL_DIR = os.path.join(data_dir, 'soil')
        ncs = glob(os.path.join(SOIL_DIR, '*.zarr'))
        ds_soil = xr.open_mfdataset(
                ncs,
                engine="zarr",
                combine="by_coords",        # fusionne par coordonnées compatibles
                parallel=True               # active Dask
        )
        if shapefile == 1:
            ds_soil = ds_soil.rio.write_crs("EPSG:4326")
            ds_soil = ds_soil.rio.clip(gdf.geometry, gdf.crs, drop=True)
        else: ds_soil = ds_soil.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        
        mask_soil = xr.concat([ds_soil[var].isnull() for var in ds_soil.data_vars], dim='variable').any(dim='variable')
        ds_mask['mask'] = ds_mask['mask'].where(~mask_soil, np.nan)
        ds_mask.to_netcdf(os.path.join(EXP_DIR, 'mask_after_soil.nc'))
        
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

        
        # Possibility to build the cropmanagement table by scratch:
        if scratch == 1: #TODO
            df_mangt = pd.DataFrame(columns=['idMangt', 'Idcultivar', 'sowingdate', 'sdens', 'OFertiPolicyCode', 'InoFertiPolicyCode', "IrrigationPolicyCode", "SoilTillPolicyCode"])
        
        
        print(df_mangt.head(10))
        spatialized = False
        
        if s_variety == 1:
            # Read the spatialized variety file
            nc_variety_file = glob(os.path.join(work_dir, 'data', 'gridded_data', 'variety', '*.nc'))[0]
        if s_fert == 1:
            # Read the spatialized fertilization file
            nc_fert_file = glob(os.path.join(work_dir, 'data', 'gridded_data', 'fert', '*.nc'))[0]
        if s_irrig == 1:
            # Read the spatialized irrigation file
            nc_irrig_file = glob(os.path.join(work_dir, 'data', 'gridded_data', 'irrig', '*.nc'))[0]
        if s_sowing == 1:
            # Read the spatialized sowing file
            nc_sowing_file = glob(os.path.join(work_dir, 'data', 'gridded_data', 'sowing', '*.nc'))[0]
        if s_density == 1:
            # Read the spatialized density file
            nc_density_file = glob(os.path.join(work_dir, 'data', 'gridded_data', 'density', '*.nc'))[0]
        
        nc_files = [nc_variety_file, nc_fert_file, nc_irrig_file, nc_sowing_file, nc_density_file]
        spatialized_files = []
        if nc_files.count(None) == len(nc_files):
            print("No spatialized file selected")
        else:
            spatialized = True
            print("Selected spatialized files:")
            for nc_file in nc_files:
                if nc_file is not None:
                    print(f"- {nc_file}")
                    spatialized_files.append(nc_file)
        
        # Read the spatialized files
        
        if spatialized_files :
            nc_spacialized = xr.open_mfdataset(spatialized_files, combine="by_coords", chunks="auto", parallel = False)
            print(nc_spacialized)
            if shapefile == 1:
                shapefile_path = glob(os.path.join(work_dir,'data', 'shapefile', '*.shp'))[0]
                gdf = gpd.read_file(shapefile_path)
                nc_spacialized = nc_spacialized.rio.write_crs("EPSG:4326")
                nc_spacialized = nc_spacialized.rio.clip(gdf.geometry, gdf.crs, drop=True)
            else: nc_spacialized = nc_spacialized.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
            
            #save the spatialized data before applying the mask
            nc_spacialized.to_netcdf(os.path.join(EXP_DIR, 'spatialized_before_mask.nc'))

            nc_spacialized = nc_spacialized.reindex({'lat': sorted(nc_spacialized.lat)})
            nc_spacialized.coords['mask'] = (('lat', 'lon'), ds_mask.mask.to_masked_array(copy=True))
            nc_spacialized = nc_spacialized.where(nc_spacialized.mask == 1, drop=True)
            print(nc_spacialized)
            nc_spacialized = nc_spacialized.drop_vars(["crs"], errors='ignore')
            
            # Save the spatialized data after applying the mask
            nc_spacialized.to_netcdf(os.path.join(EXP_DIR, 'spatialized_after_mask.nc'))

            # Convert the spatialized data to a pandas dataframe
            df_spatialized = nc_spacialized.to_dataframe().dropna(axis=0, how="any")
            print(df_spatialized.head(10))
        
            if s_variety == 1:
                #replace the variety values with the values from the variety dictionary: but convert first df_spatialized['variety'] to string
                # Arrondir pour éviter les erreurs de précision flottante (25.999999996 -> 26.0)
                df_spatialized['variety'] = df_spatialized['variety'].round(0).astype(str)
                # Assurer le format "X.0" pour matcher les clés du dictionnaire
                df_spatialized['variety'] = df_spatialized['variety'].apply(lambda x: x if '.0' in x else x + '.0')
                df_spatialized['variety'] = df_spatialized['variety'].map(variety_dict)
                # replace colum name "variety" by "Idcultivar"
                df_spatialized = df_spatialized.rename(columns={"variety":"Idcultivar"})
                # reset the index
                
                # create a new column "id"  based on lat and lon columns 4 digits after the decimal point
                
            if s_sowing == 1:
                df_spatialized = df_spatialized.rename(columns={"sowing_date": "sowingdate"})
                df_mangt.drop("sowingdate", axis=1, inplace=True)

            if s_density == 1:
                df_mangt.drop("sdens", axis=1, inplace=True)

            if s_fert == 1:
                df_mangt.drop("InoFertiPolicyCode", axis=1, inplace=True)
            
            df_spatialized = df_spatialized.reset_index()
            df_spatialized["idPoint"] = df_spatialized["lat"].round(4).astype(str) + '_' + df_spatialized["lon"].round(4).astype(str)
        
            if s_variety == 1:
                # keep only the Idcultivar conatined in the variety dictionary
                df_mangt.drop("Idcultivar", axis=1, inplace=True)
                #df_mangt = df_mangt[df_mangt['Idcultivar'].isin(variety_dict.values())]
                #merge the spatialized data with the CropManagement table based on the Idcultivar column
                #df_mangt = pd.merge(df_mangt, df_spatialized, on="Idcultivar", how="inner")
            #else:
                # cross join the CropManagement table with the spatialized data
            df_mangt["key"] = 0
            df_spatialized["key"] = 0
            df_mangt = df_mangt.merge(df_spatialized, on="key")
            df_mangt.drop("key", axis=1, inplace=True)
            # remove rows without values in Idcultivar column
            #df_mangt = df_mangt.loc[df_mangt['Idcultivar'].notna()].copy()                
            
        
            print(df_mangt.head(10))
        
           
            # convert sowing_date as int
            df_mangt["sowingdate"] = df_mangt["sowingdate"].astype(int)
            
        
            # replace idMangt column by the concatenation of id and idMangt columns if len(spatialized_files) != 5
            if len(spatialized_files) != 5:
                df_mangt["oldid"] = df_mangt["idMangt"]
                df_mangt["idMangt"] = df_mangt['idPoint'] + "_" + df_mangt["idMangt"]

            
        if int(sd) == 3:
            # use sw the list of sowing dates
            df_mangt.drop("sowingdate", axis=1, inplace=True)
            # Make a cartesian product between the CropManagement table and the list of sowing dates
            df_sowing = pd.DataFrame()
            df_sowing["sowingdate"] = sw
            df_sowing["key"] = 0
            df_mangt["key"] = 0
            df_mangt = df_mangt.merge(df_sowing, on="key")
            df_mangt.drop("key", axis=1, inplace=True)
            df_mangt["sowingdate"] = df_mangt["sowingdate"].astype(int)
            # Change the values of the column idMangt with the values of idMangt + "_" + sowingdate
            df_mangt["idMangt"] = df_mangt["idMangt"] + "_" + df_mangt["sowingdate"].astype(str)
            if spatialized: df_mangt["oldid"] = df_mangt["oldid"] + "_" + df_mangt["sowingdate"].astype(str)
            
        if s_variety == 0:
            # No spatialized variety file. select in cropmanagement the variety in the list cropvariety
            print("variety", variety)
            if len(variety) != 0:
                df_mangt = df_mangt[df_mangt['Idcultivar'].isin(variety)]
        
        if s_fert == 0:
            # No spatialized fertilization file. select in cropmanagement the fertilization in the list ferti
            if len(fertioption) != 0:
                df_mangt = df_mangt[df_mangt['InoFertiPolicyCode'].isin(fertioption)]
               
        with sqlite3.connect(DB_MI) as conn:
            print("test cropmanagement", df_mangt.head(10))
            cur = conn.cursor()
            df_mangt.to_sql("CropManagement", conn, if_exists='replace', index=False)                
            conn.commit()    
    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
