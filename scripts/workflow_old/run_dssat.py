import sqlite3
import pandas as pd
import os
import shutil
import subprocess
import argparse
from pathlib import Path
import zipfile
from joblib import Parallel, delayed
import multiprocessing
from glob import glob
import numpy as np
import time
from modfilegen.Converter.DssatConverter import dssatconverter
from modfilegen import GlobalVariables
import concurrent.futures
import cProfile
import pstats
import calendar

import traceback
import sys
import re
    

def extract_corrected_doy(date_col, ys):

    # Extract year and DOY
    year = (date_col // 1000).astype('float')
    doy = (date_col % 1000).astype('float')

    # Add 365 or 366 if year > ys
    correction = np.where(
        year > ys,
        np.where([calendar.isleap(int(y)) if not np.isnan(y) else False for y in year],
                 366, 365),
        0
    )

    return doy + correction


def main():


    work_dir = '/package' 
    inter = '/inter'
    parser = argparse.ArgumentParser(description='load etp into database')
    parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
    parser.add_argument('--ncpus', help="number of cpus by task")
    parser.add_argument('--testoption', help="option test")
    args = parser.parse_args()
    i = args.index
    size = int(args.ncpus)
    testoption = int(args.testoption)
    EXPS_DIR = os.path.join(inter, 'EXPS')
    EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
    out = "/outputData" 
    output_dir = os.path.join(out, 'EXPS', 'exp_' + str(i))

    DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
    DB_MD = os.path.join(EXP_DIR, "ModelsDictionaryArise.db")

    directoryPath = os.path.join(EXP_DIR, "output")
    if not os.path.exists(directoryPath):
        Path(directoryPath).mkdir(parents=True, exist_ok=True)

    GlobalVariables["dbModelsDictionary" ] = DB_MD     
    GlobalVariables["dbMasterInput" ] = DB_MI
    GlobalVariables["directorypath"] = directoryPath 
    GlobalVariables["pltfolder"] = os.path.join(work_dir, "data","cultivars","dssat") # path of cultivars
    GlobalVariables["nthreads"] = size
    GlobalVariables["dt"] = 0

    dssatconverter.main()
        
    # read in directorypath all the files end with "dssat.csv" and concatenate them
    files = glob(os.path.join(directoryPath, '*_dssat.csv'))
    if not files: return
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    print(f"Number of effective simulations in this subdomain: {len(df)}")
    if testoption!=1: shutil.rmtree(directoryPath)

    v = list(set(["_".join(u.split("_")[3:]) for u in df["Idsim"]]))
    
    # get the year of simulation from df["IdSim"]
    df["ys"] = (df["Idsim"].str.split("_").str[2]).astype(int)
    print(f"Year of simulation: {df['ys']}")
    
    df = df.replace(-99, np.nan)

    for col in ["Planting","Emergence","Ant","Mat"]:
        df[col] = extract_corrected_doy(df[col], df["ys"])

    for col in ["Yield", "Biom_ma"]:
        df[col] = df[col] / 1000  # Conversion kg -> tonnes

    cols_to_clean = ["Planting", "Emergence", "Ant", "Mat", "Biom_ma", "Yield", "GNumber",
                    "MaxLai", "Nleac", "SoilN", "CroN_ma", "CumE", "Transp"]

    df[cols_to_clean] = df[cols_to_clean].mask(df[cols_to_clean] < 0, np.nan)

    def create_netcdf(id_, df):
        df_2 = df[df["Idsim"].str.endswith(id_)]
        dsfin = df_2[["time","lat","lon","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","Nleac","SoilN","CroN_ma","CumE","Transp"]]
        dsfin = dsfin.reset_index().set_index(
                        ['time', 'lat', 'lon']).to_xarray()
        o = os.path.join(output_dir, 'dssat' + '_yearly_' + id_ + "_" + str(i) + '.nc')
        dsfin.to_netcdf(o)
    
    njobs = len(v) if len(v) < size else size
    Parallel(n_jobs=njobs)(delayed(create_netcdf)(f, df) for f in v)
    df.reset_index()

    if testoption==1:
        df = df[["Model","Idsim","Texte","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","Nleac","SoilN","CroN_ma","CumE","Transp"]]
            
        with sqlite3.connect(DB_MI, timeout=15) as c:
            cur = c.cursor()
            cur.executescript("DELETE FROM SummaryOutput WHERE Model='Dssat';")
            c.commit()
            df.to_sql('SummaryOutput', c, if_exists='append', index=False)
            c.commit()

    else: os.remove(DB_MI)
    #if inter!=out: os.remove(inter)

    print("DONE!")       
    
if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)  # top 20 fonctions les plus lentes
