#!/usr/bin/env python

import os
import xarray as xr
from glob import glob
import sys
import traceback
from joblib import Parallel, delayed
import argparse
import numpy as np
import geopandas as gpd
import rioxarray
import dask
from xarray import open_zarr

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


def main():
    outdir = '/outputData'
    data_dir = '/inputData'
    work_dir = '/package'
    parser = argparse.ArgumentParser(description='load soil data into database')
    parser.add_argument("--bnd", nargs="+", type=float, help="area bound")
    parser.add_argument("--models", nargs="+", type=str, help="Specify the models") 
    parser.add_argument("--shp", help="Specify the shapefile")
    parser.add_argument("--cropmask", help="Specify the shapefile")
    parser.add_argument("--ncpus", type=int, default=4, help="Number of CPU cores to use")
    args = parser.parse_args()
    bound = args.bnd
    cropmask = args.cropmask
    models = args.models
    shp = int(args.shp)
    ncpus = args.ncpus
    # Load reference grid (ensures all expected pixels are there)
    ds = xr.open_zarr(glob(os.path.join(data_dir, 'meteo', 'Rainfall', '*.zarr'))[0], consolidated=True).chunk({'time': 365, 'lat': 'auto', 'lon': 'auto'})
    ds = ds.isel(time=0)   
    print("Reference dataset loaded", flush=True) 
    if shp == 1:
        shapefile_path = glob(os.path.join(work_dir,'data','shapefile', '*.shp'))[0]
        print("Using shapefile:", shapefile_path)
        gdf = gpd.read_file(shapefile_path)
        ds = ds.rio.write_crs("EPSG:4326")
        ds = ds.rio.clip(gdf.geometry, gdf.crs,  all_touched=True, drop=True)
    else:
        print("using bbox", bound[1],bound[3],bound[1],bound[3], flush = True)
        ds = ds.sel(lat=slice(bound[1],bound[3]), lon=slice(bound[0], bound[2]))
        ds = ds.drop_vars([v for v in ['spatial_ref', 'crs'] if v in ds])
        ds = ds.reindex({'lat': sorted(ds.lat)})
    ref_ds_clipped = ds
    ref_ds_clipped['lat'] = np.round(ref_ds_clipped['lat'], 4)
    ref_ds_clipped['lon'] = np.round(ref_ds_clipped['lon'], 4)    
    EXPS_DIR = os.path.join(outdir, 'EXPS')
    if cropmask == 1:
        mask_ds = ds = xr.open_zarr(glob(os.path.join(data_dir, 'land', '*.zarr'))[0], consolidated=True)
        mask_ds = mask_ds.reindex(lat=ref_ds_clipped.lat, lon=ref_ds_clipped.lon)
        crop_mask = mask_ds['mask']
        print("Crop mask loaded and reindexed", flush=True)
        print(f"Crop mask shape: {crop_mask.sizes}, {crop_mask}", flush=True)
        
    clear_memory()
    print("Memory cleared", flush=True)
    log_memory()
    
    def merge_one_scenario(m, n):
        try:
            print(f"Merging datasets for model: {m}, scenario: {n}", flush=True)
            subdomain_files = glob(os.path.join(EXPS_DIR,'exp_*', m+'_yearly_'+n+'_*.nc'))
            if len(subdomain_files)==0: return
            final_ds = None
            for i, f in enumerate(subdomain_files):
                #print(f"Processing {f}", flush=True)
                ds = xr.open_dataset(f).load()
                ds = ds.reindex(lat=ref_ds_clipped.lat, lon=ref_ds_clipped.lon)
                if final_ds is None:
                    final_ds = ds
                else:
                    final_ds = final_ds.combine_first(ds) 
                ds.close()
            #print(f"Merged dataset shape: {final_ds.sizes}", flush=True)
            for var in final_ds.data_vars:
                if np.issubdtype(final_ds[var].dtype, np.floating):
                    final_ds[var] = final_ds[var].astype("float32")   
            if cropmask == 1:
                for var in final_ds.data_vars:
                    data = final_ds[var].values
                    condition = np.isnan(data) & (crop_mask.values == 1)
                    data[condition] = 0
                    final_ds[var].values = data
            '''else:
                for var in final_ds.data_vars:
                    data = final_ds[var].values
                    condition = np.isnan(data) & (~np.isnan(ref_ds_clipped["pr"].values))
                    data[condition] = 0
                    final_ds[var].values = data'''            
            # Save merged dataset to NetCDF file
            out = os.path.join(outdir,"outputs")
            os.makedirs(out, exist_ok=True)
            output_file = os.path.join(out, m+'_yearly_'+n+'.nc')
            final_ds.to_netcdf(output_file, encoding={var: {"_FillValue": np.nan} for var in final_ds.data_vars})
            print(f"Merged dataset saved as {output_file}")
            final_ds.close()
            clear_memory()
            log_memory()
            print(f"Finished processing model: {m}, scenario: {n}", flush=True)

        except:
            print("Unexpected error have been catched:", sys.exc_info()[0])
            traceback.print_exc()
            sys.exit(1)
    
    for m in models:
        print(f"Processing model: {m}", flush=True)
        ncs = glob(os.path.join(EXPS_DIR,'exp_*', m+'_yearly_*.nc'))
        v = list(set(["_".join(os.path.basename(u).split("_")[2:-1]) for u in ncs]))
        work_items = len(v)
        print(f"number of scenarios {work_items}", flush=True)
        
        if work_items == 0:
            print(f"No scenarios found for {m}", flush=True)
            continue
        n_workers = min(ncpus, work_items)
        Parallel(n_jobs=n_workers)(
            delayed(merge_one_scenario)(m, n) for n in v)
        clear_memory()

if __name__ == "__main__":
    main()        

