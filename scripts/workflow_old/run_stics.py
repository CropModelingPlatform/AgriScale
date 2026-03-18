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
import time


import traceback
import sys
import re

from modfilegen import GlobalVariables  
from modfilegen.Converter.SticsConverter import sticsconverter


def main():

    work_dir = '/package' 
    inter = '/inter' 
    temp = '/tempDir'
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
    GlobalVariables["pltfolder"] = os.path.join(work_dir, "data","cultivars","stics") # path of cultivars
    GlobalVariables["nthreads"] = size
    GlobalVariables["dt"] = 0
    GlobalVariables["tempDir"] = directoryPath
    GlobalVariables["parts"] = 1
    start = time.time()
    sticsconverter.main()
    print(f'time of simulation {time.time() - start}')
    files = glob(os.path.join(directoryPath, '*_stics.csv'))
    if len(files) == 0:
        print("No simulation files found in the output directory.")
        return
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    print(f"Number of effective simulations in this subdomain: {len(df)}")
    if testoption!=1: shutil.rmtree(directoryPath)
   
    v = list(set(["_".join(u.split("_")[3:]) for u in df["Idsim"]]))    
    def create_netcdf(id_, dffin):
        df_2 = dffin[dffin["Idsim"].str.endswith(id_)]
        dsfin = df_2[["time","lat","lon","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","Nleac","SoilN","CroN_ma","CumE","Transp"]]    
        dsfin = dsfin.reset_index().set_index(['time', 'lat', 'lon']).to_xarray()
        o = os.path.join(output_dir, 'stics' + '_yearly_' + id_ + "_" + str(i) + '.nc')
        dsfin.to_netcdf(o)

    njobs = size
    Parallel(n_jobs=njobs)(delayed(create_netcdf)(f, df) for f in v)
    df.reset_index()
    if testoption==1:
        df = df[["Model","Idsim","Texte","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","Nleac","SoilN","CroN_ma","CumE","Transp"]]
            
        with sqlite3.connect(DB_MI, timeout=15) as c:
            cur = c.cursor()
            cur.executescript("DELETE FROM SummaryOutput WHERE Model='Stics';")
            c.commit()
            df.to_sql('SummaryOutput', c, if_exists='append', index=False)
            c.commit()
    print("DONE!")
    #os.remove(DB_MI)  
    
if __name__ == "__main__":

    main()

