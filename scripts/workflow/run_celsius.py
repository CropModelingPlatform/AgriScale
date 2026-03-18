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
from modfilegen.Converter.CelsiusConverter import celsiusconverter
from modfilegen import GlobalVariables
import sys, traceback
import cProfile
import pstats



def get_lon(d):
    res_=d.split("_")
    lon_=float(res_[1])
    return lon_

def get_lat(d):
    res_=d.split("_")
    lat_=float(res_[0])
    return lat_

def get_time(d):
    res_=d.split("_")
    year_ = int(float(res_[2]))
    return year_ 
    


def main():

    try:
        work_dir = '/package' 
        inter = '/inter' 
        parser = argparse.ArgumentParser(description='load etp into database')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        parser.add_argument('--ncpus', help="number of cpus by task")
        parser.add_argument('--testoption', help="option test")
        parser.add_argument('--parts', help="parts")
        args = parser.parse_args()
        i = args.index
        size = int(args.ncpus)
        testoption = int(args.testoption)
        parts = int(args.parts)
        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        out = "/outputData" 
        output_dir = os.path.join(out, 'EXPS', 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
        DB_MD = os.path.join(EXP_DIR, "ModelsDictionaryArise.db")
        DB_CEL = os.path.join(EXP_DIR, "CelsiusV3nov17_dataArise.db")
        ORI_MI = os.path.join(work_dir, "db",  'MasterInput.db')
        directoryPath = os.path.join(EXP_DIR, "celsius", "output")
        if not os.path.exists(directoryPath):
            Path(directoryPath).mkdir(parents=True, exist_ok=True)

        GlobalVariables["dbModelsDictionary" ] = DB_MD     
        GlobalVariables["dbMasterInput" ] = DB_MI
        GlobalVariables["directorypath"] = directoryPath 
        GlobalVariables["nthreads"] = size
        GlobalVariables["dt"] = 1- testoption 
        GlobalVariables["ori_MI"] = ORI_MI
        GlobalVariables["dbCelsius"] = DB_CEL
        GlobalVariables["parts"] = parts

        
        print('EXP_DIR : ' + EXP_DIR)
        sqlite_connection_celsius = sqlite3.connect(DB_CEL)

        start = time.time()
        celsiusconverter.main()
        print(f'time of simulation {time.time() - start}')
        if testoption!=1: shutil.rmtree(directoryPath)
        
        df = pd.read_sql('SELECT * FROM OutputSynt', sqlite_connection_celsius)
        df = df.reset_index().rename(columns={"idsim":"Idsim","iplt":"Planting","JulPheno1_1":"Emergence","JulPheno1_4":"Ant","JulPheno1_6":"Mat","Biom(nrec)":"Biom_ma","Grain(nrec)":"Yield","LAI":"MaxLai","SigmaSimEsol":"CumE","Ngrain":"GNumber","stockNsol":"SoilN","SigmaCultEsol":"Transp"})
        df["Model"] = "Celsius" 
        df["Texte"] = ""     
            
        df['time'] = df.apply(lambda x: get_time(x['Idsim']), axis=1)
        df['lon'] = df.apply(lambda x: get_lon(x['Idsim']), axis=1)
        df['lat'] = df.apply(lambda x: get_lat(x['Idsim']), axis=1)
            
        v = list(set(["_".join(u.split("_")[3:]) for u in df["Idsim"]]))
            
        def create_netcdf(id_, df):
            df_2 = df[df["Idsim"].str.endswith(id_)]

            dsfin = df_2[["time", "lat", "lon", "Planting", "Emergence", "Ant", "Mat",
                                    "Biom_ma", "Yield", "GNumber", "MaxLai", "SoilN", "CumE", "Transp"]]
            dsfin = dsfin.reset_index().set_index(
                            ['time', 'lat', 'lon']).to_xarray()
            o = os.path.join(output_dir, 'celsius' + '_yearly_' + id_ + "_" + str(i) + '.nc')
            dsfin.to_netcdf(o)
            
        Parallel(n_jobs=size)(
            delayed(create_netcdf)(f, df) for f in v)
        sqlite_connection_celsius.close()

        if testoption==1:
            df = df[["Model","Idsim","Texte","Planting","Emergence","Ant","Mat","Biom_ma","Yield","GNumber","MaxLai","SoilN","CumE","Transp"]]        
            
            with sqlite3.connect(DB_MI, timeout=10) as c:
                cur = c.cursor()
                cur.execute("DELETE FROM SummaryOutput WHERE Model='Celsius';")
                c.commit()
                df.to_sql('SummaryOutput', c, if_exists='append', index=False)
                c.commit()
        else: os.remove(DB_CEL)  
        #os.remove(DB_MI)
        #if inter!=out: os.remove(inter)
        print("DONE!")

    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)
    
if __name__ == "__main__":

    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(20)  # top 20 fonctions les plus lentes

