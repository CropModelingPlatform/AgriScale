import numpy as np
import sqlite3
import calculate_etp
from joblib import Parallel, delayed
import argparse
import sys
import traceback
import os
import pandas as pd

def Ra_vectorized(lat, J):
    pi = np.pi
    latrad = np.radians(lat)
    Dr = 1 + 0.033 * np.cos(2 * pi * J / 365)
    Declin = 0.409 * np.sin((2 * pi * J / 365) - 1.39)
    SolarAngle = np.arccos(-np.tan(latrad) * np.tan(Declin))
    Ra = (24 * 60 / pi) * 0.082 * Dr * (
        SolarAngle * np.sin(latrad) * np.sin(Declin) +
        np.cos(latrad) * np.cos(Declin) * np.sin(SolarAngle)
    )
    return Ra


def ET0pm_Tdew_vectorized(lat, Alt, J, Tn, Tx, Tm, Rhum, Vm, Rg):
    sigma = 4.903e-9

    # Avoid division by zero or NaNs
    Tn = np.asarray(Tn, dtype=np.float64)
    Tx = np.asarray(Tx, dtype=np.float64)
    Tm = np.asarray(Tm, dtype=np.float64)
    Rhum = np.asarray(Rhum, dtype=np.float64)
    Vm = np.asarray(Vm, dtype=np.float64)
    Rg = np.asarray(Rg, dtype=np.float64)
    Alt = np.asarray(Alt, dtype=np.float64)
    lat = np.asarray(lat, dtype=np.float64)
    J = np.asarray(J, dtype=np.float64)

    gamma = 101.3 * ((293 - 0.0065 * Alt) / 293) ** 5.26
    gamma = 0.000665 * gamma

    E0SatTn = 0.6108 * np.exp(17.27 * Tn / (Tn + 237.3))
    E0SatTx = 0.6108 * np.exp(17.27 * Tx / (Tx + 237.3))
    SlopeSat = 4098 * (0.6108 * np.exp(17.27 * Tm / (Tm + 237.3))) / ((Tm + 237.3) ** 2)

    '''Ea = 0.5 * 0.6108 * (
        np.exp(17.27 * Tdewx / (Tdewx + 237.3)) +
        np.exp(17.27 * Tdewn / (Tdewn + 237.3))
    )'''
    Ea = (Rhum * ((E0SatTn + E0SatTx) / 2))/100

    VPD = ((E0SatTn + E0SatTx) / 2) - Ea
    adv = gamma * 900 * Vm * VPD / (Tm + 273)

    Ra_vals = Ra_vectorized(lat, J)
    Rso = (0.00002 * Alt + 0.75) * Ra_vals
    Rns = (1 - 0.23) * Rg
    Rnl_ratio = Rg / Rso
    Rnl_ratio = np.clip(Rnl_ratio, 0, 1)  # Ensure between 0 and 1

    Rnl = (Rnl_ratio * 1.35 - 0.35) * (-0.14 * np.sqrt(Ea) + 0.34)

    Tn_K = Tn + 273.16
    Tx_K = Tx + 273.16
    Rnl = sigma * (Tn_K ** 4 + Tx_K ** 4) * Rnl / 2

    Rad = 0.408 * SlopeSat * (Rns - Rnl)
    ET0 = (Rad + adv) / (SlopeSat + gamma * (0.34 * Vm + 1))

    return ET0

def main():
    try:
        parser = argparse.ArgumentParser(description='Compute ETP and store in database')
        parser.add_argument('-i', '--index', required=True, help="Index of the sub virtual experience")
        parser.add_argument('--ncpus', required=True, type=int, help="Number of CPUs")
        args = parser.parse_args()

        work_dir = '/package'
        inter = '/inter'
        EXP_DIR = os.path.join(inter, 'EXPS', f'exp_{args.index}')
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')

        conn = sqlite3.connect(DB_MI)
        conn.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA synchronous=NORMAL;
                PRAGMA temp_store=MEMORY;
            """)
        coords = pd.read_sql("SELECT idPoint, latitudeDD as latitude, altitude FROM Coordinates", conn)

        chunk_iter = pd.read_sql_query("SELECT * FROM RAclimateD", conn, chunksize=200_000)
        print("Starting ETP computation", flush=True)
        for df_chunk in chunk_iter:
            df_chunk = df_chunk.merge(coords, on="idPoint", how="left")
            df_chunk["Etppm"] = ET0pm_Tdew_vectorized(
                df_chunk["latitude"].values,
                df_chunk["altitude"].values,
                df_chunk["DOY"].values,
                df_chunk["tmin"].values,
                df_chunk["tmax"].values,
                df_chunk["tmoy"].values,
                df_chunk["rhum"].values,
                df_chunk["wind"].values,
                df_chunk["srad"].values
            )
            # drop latitude and altitude columns as they are no longer needed
            df_chunk.drop(columns=["latitude", "altitude"], inplace=True)
            print(df_chunk.head(5), flush=True)
            # Write results into a new table or temporary table
            df_chunk.to_sql("RAclimateD_with_etp", conn, if_exists="append", index=False, method="multi", chunksize=1000)
        
        conn.execute("DROP TABLE RAclimateD;")
        conn.execute("ALTER TABLE RAclimateD_with_etp RENAME TO RAclimateD;")        
        conn.close()

        # VACUUM: reclaim space & defragment
        with sqlite3.connect(DB_MI, timeout=30) as c:
            c.execute("VACUUM;")
            c.execute("PRAGMA optimize;")  # optional


    except Exception as e:
        print("❌ Unexpected error caught:", flush=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
