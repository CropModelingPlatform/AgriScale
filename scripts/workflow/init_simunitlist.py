#!/usr/bin/env python

import os
import sqlite3
import argparse
import sys
import traceback
import cProfile
import pstats
import pandas as pd


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
    try:
        print("init_simunitlist.py")
        work_dir = '/package'
        inter = '/inter'
        parser = argparse.ArgumentParser(description='load soil data into database')

        parser.add_argument("--index", type=int, help="Index value")
        parser.add_argument("--startdate", type=int, help="Start date")
        parser.add_argument("--enddate", type=int, help="End date")
        parser.add_argument("--option", nargs="+", type=int, help="Simulation option") 
        parser.add_argument("--sowingoption", type=int, help="sowing option")
        parser.add_argument("--deltaStart", type=int, help="diff between sowing date and start of simulation")
        parser.add_argument("--deltaEnd", type=int, help="diff between sowing date and end of simulation")

        args = parser.parse_args()
        
        i = args.index
        startd = args.startdate
        endd = args.enddate
        simoption = args.option
        sd = args.sowingoption
        print(simoption)
       
        #o = args.option
        EXPS_DIR = os.path.join(inter, 'EXPS')
        EXP_DIR = os.path.join(EXPS_DIR, 'exp_' + str(i))
        DB_MI = os.path.join(EXP_DIR, 'MasterInput.db')
        DB_Celsius = os.path.join(EXP_DIR, 'CelsiusV3nov17_dataArise.db')
        print(f"value of sd {sd} type(sd)")
        log_memory()
        if sd==1:
            with sqlite3.connect(DB_Celsius, timeout=15) as c:
                cur = c.cursor()
                cur.execute(f'update OptionsModel set CyberST=1')
                c.commit()  
            
        init_sql = """DELETE FROM Coordinate_years;
        INSERT INTO Coordinate_years
        SELECT distinct idPoint, CAST(year AS INTEGER)
        FROM RAClimateD;"""
        
        p_d = int(args.deltaStart)
        p_e = int(args.deltaEnd)
        if sd == 4 or sd == 3 or sd==0:
            start_day_expr = "CASE WHEN cm.sowingdate - ? < 1 THEN 1 ELSE cm.sowingdate - ? END"
            end_day_expr = f"""
                CASE 
                    WHEN cm.sowingdate + ? <= 365 THEN cm.sowingdate + ? 
                    WHEN (cy.year % 4 = 0 AND cy.year % 100 != 0) OR (cy.year % 400 = 0) THEN cm.sowingdate + ? - 366 
                    ELSE cm.sowingdate + ? - 365 
                END
            """
            end_year_expr = "CASE WHEN cm.sowingdate + ? > 365 THEN cy.year + 1 ELSE cy.year END"
            params = (p_d, p_d, p_e, p_e, p_e, p_e, p_e) 
        else:
            start_day_expr = "?"
            end_day_expr =  "?"
            end_year_expr = f"""
                CASE 
                    WHEN ? > ? THEN cy.year + 1
                    ELSE cy.year
		END
            """
            params =  (startd, endd, startd, endd) 

        sql_as_string_spatialized = f"""
                SELECT DISTINCT
                    so.idOption AS idOption,
                    cy.idPoint,
                    cm.idMangt,
                    s.IdSoil,
                    CAST(cy.year AS INTEGER) AS StartYear,
                    {start_day_expr} AS StartDay, 
                    {end_year_expr} AS EndYear,
                    {end_day_expr} AS EndDay, 
                    1 AS idIni,  
                    cy.idPoint || '_' || cy.year || '_' || cm.oldid || '_' || so.idOption AS idsim
                FROM 
                    Coordinate_years cy
                    INNER JOIN Soil s ON cy.idPoint = s.idSoil
                    INNER JOIN CropManagement cm ON cy.idPoint = cm.idPoint
                    JOIN temp_simoption so
                GROUP BY 
                    cy.idPoint, cm.idMangt, s.IdSoil, cy.year, so.idoption
                ORDER BY 
                    cy.idPoint, cm.idMangt, s.IdSoil, cy.year, so.idOption;
        
        """
        #INSERT INTO SimUnitList (idOption, idPoint, idMangt, IdSoil, StartYear, StartDay, EndYear, EndDay, idIni, idsim)
        #INSERT INTO SimUnitList (idOption, idPoint, idMangt, IdSoil, StartYear, StartDay, EndYear, EndDay, idIni, idsim)
        sql_as_string = f"""
                SELECT DISTINCT
                    so.idOption AS idOption,
                    cy.idPoint,
                    cm.idMangt,
                    s.IdSoil,
                    CAST(cy.year AS INTEGER) AS StartYear,
                    {start_day_expr} AS StartDay, 
                    {end_year_expr} AS EndYear,
                    {end_day_expr} AS EndDay, 
                    1 AS idIni,
                    cy.idPoint || '_' || cy.year || '_' || cm.idMangt || '_' || so.idOption AS idsim
                FROM 
                    CropManagement cm,    
                    temp_simoption so,
                    Coordinate_years cy
                    INNER JOIN Soil s ON cy.idPoint = s.idSoil
                GROUP BY 
                    cy.idPoint, cm.idMangt, s.IdSoil, cy.year, so.idOption
                ORDER BY 
                    cy.idPoint, cm.idMangt, s.IdSoil, cy.year, so.idOption;
            """
        
        with sqlite3.connect(DB_MI) as conn:
            cur = conn.cursor()
            conn.executescript("""
                PRAGMA journal_mode=DELETE;
                PRAGMA synchronous=OFF;
                PRAGMA temp_store=MEMORY;
            """)


            # Create indexes on the relevant columns
            """cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_IdOptions'")     #### remove index creation
            existing_index = cur.fetchone()
            if not existing_index:
                 cur.execute("CREATE INDEX idx_IdOptions ON SimulationOptions(IdOptions);")

            cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_idPoint'")
            existing_index = cur.fetchone()
            if not existing_index:
                 cur.execute("CREATE INDEX idx_idPoint ON Coordinate_years(idPoint);")

            cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_idMangt'")
            existing_index = cur.fetchone()
            if not existing_index:
                 cur.execute("CREATE INDEX idx_idMangt ON CropManagement(idMangt);")

            cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_IdSoil'")
            existing_index = cur.fetchone()
            if not existing_index:
                 cur.execute("CREATE INDEX idx_IdSoil ON Soil(IdSoil);")

            cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_idPoint2'")
            existing_index = cur.fetchone()
            if not existing_index:
                 cur.execute("CREATE INDEX idx_idPoint2 ON RAClimateD(idPoint);")"""
            
            conn.commit()

            cur.execute('CREATE TEMPORARY TABLE temp_simoption (idoption TEXT)')
            for idoption in simoption:
                cur.execute('INSERT INTO temp_simoption (idoption) VALUES (?)', (idoption,))

            cur.executescript(init_sql)
            cur.executescript("DELETE FROM SimUnitList")

            cur.execute("PRAGMA table_info(CropManagement)")
            columns = [col[1] for col in cur.fetchall()]
            idPoint_exists = "idPoint" in columns
            
            print("idPoint_exists", idPoint_exists)
            log_memory()
            if idPoint_exists:
                select_sql = sql_as_string_spatialized
            else:
                select_sql = sql_as_string
                
            for chunk in pd.read_sql_query(select_sql, conn, params=params, chunksize=1000):
                log_memory()
                rows = list(chunk.itertuples(index=False, name=None))
                    
                insert_stmt = """
                        INSERT INTO SimUnitList (
                            idOption, idPoint, idMangt, IdSoil, StartYear, StartDay,
                            EndYear, EndDay, idIni, idsim
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                cur.executemany(insert_stmt, rows)
                
                conn.commit()
                log_memory()
                print(f"Inserted {len(rows)} rows.")
            cur.execute("DROP TABLE IF EXISTS temp_simoption")

            # Filtrage post-insertion : supprimer les lignes où idOption=0 et idMangt n'est pas associée à InoFertiPolicyCode=0
            cur.execute('''
                DELETE FROM SimUnitList
                WHERE (idOption = 2 OR idOption = 4) 
                AND idMangt NOT IN (
                    SELECT idMangt FROM CropManagement WHERE InoFertiPolicyCode = 0
                )
            ''')
            conn.commit()

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
