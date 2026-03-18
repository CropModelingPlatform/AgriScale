
import sqlite3
import argparse
import sys
import traceback
import os
import pandas as pd


def create_idJourClim(row):
    return row['IdDClim'] + '.' + str(row['annee']) + '.' + str(row['jda'])

def main():
    try:
        print("Transfert Climate")
        work_dir = '/package' 
        inter = '/inter'
        parser = argparse.ArgumentParser(description='load etp into database')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        args = parser.parse_args()
        i = args.index
        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))

        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
        DB_Celsius = os.path.join(EXP_DIR, 'CelsiusV3nov17_dataArise.db')

        conn_MI = sqlite3.connect(DB_MI)
        conn_Celsius = sqlite3.connect(DB_Celsius)

        df_clim_MI = pd.read_sql("Select idPoint,year,DOY,Nmonth,NdayM,srad,tmax,tmin,tmoy,rain,Etppm from RAclimateD", conn_MI)
        df = df_clim_MI.rename(columns={"idPoint":"IdDClim", "year":"annee", "DOY":"jda", "Nmonth":"mois", "NdayM":"jour", "srad":"rg", "rain":"plu", "Etppm":"Etp"})
        print(df)
        df['idjourclim'] = df.apply(create_idJourClim, axis=1)
        #df_sorted = df.sort_values(by='idjourclim')
        df = df[['IdDClim', 'idjourclim', 'annee',"jda","mois","jour","tmax","tmin","tmoy","rg","plu",'Etp' ]]
        df.to_sql('Dweather', conn_Celsius, if_exists='replace', index=False)

        create_index_query_idDclim = "CREATE INDEX IF NOT EXISTS idx_idDclim ON Dweather (IdDClim, annee);"
        cursor = conn_Celsius.cursor()
        cursor.execute(create_index_query_idDclim)

        conn_Celsius.commit()
        conn_MI.close()
        conn_Celsius.close()
        print( "transfert of climate data from MI to Cel done")
    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()