#!/usr/bin/env python

import os
import argparse
import sys
import traceback
import subprocess


def main():
    try:
        print("init.py")
        # work_dir = os.getcwd()
        work_dir = '/package'
        inter = '/inter'
        outputdir = '/outputData'
        parser = argparse.ArgumentParser(description='Initialize virtual experience directories')
        parser.add_argument('-i', '--index', help="Specify the index of the sub virtual experience")
        args = parser.parse_args()
        i = args.index
        INTER_DIR = os.path.join(inter, 'EXPS')
        OUT_DIR = os.path.join(outputdir, 'EXPS')
        interdir = os.path.join(INTER_DIR, 'exp_' + str(i))
        outdir = os.path.join(OUT_DIR, 'exp_' + str(i))

        os.makedirs(interdir, exist_ok=True)
        os.makedirs(outdir, exist_ok=True)
        print('interdir : ' + interdir)
        print('outdir : ' + outdir)
        dbfrom = os.path.join(work_dir, 'db', 'MasterInput.db')
        dbto = os.path.join(interdir, 'MasterInput.db')
        res = subprocess.check_call(['cp',  dbfrom, dbto])

        dbfrom = os.path.join(work_dir, 'db', 'CelsiusV3nov17_dataArise.db')
        dbto = os.path.join(interdir, 'CelsiusV3nov17_dataArise.db')
        res = subprocess.check_call(['cp',  dbfrom, dbto])

        dbfrom = os.path.join(work_dir, 'db', 'ModelsDictionaryArise.db')
        dbto = os.path.join(interdir, 'ModelsDictionaryArise.db')
        res = subprocess.check_call(['cp',  dbfrom, dbto])

    except:
        print("Unexpected error have been catched:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
