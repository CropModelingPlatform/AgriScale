#!/usr/bin/env bash

export INDEXES=$INDEXES #($SLURM_ARRAY_TASK_ID)
export ncpus=$ncpus # number of cpus

testoption=$1
parts=$2


if [[ -z "$DATAMILL_WORK" ]]; then
  export DATAMILL_WORK='/package'
fi

echo "INDEXES : $INDEXES"

i=$INDEXES;

work_dir='/inter'
DIR_EXP=${work_dir}/EXPS/exp_$i

cd $DIR_EXP


DB_MI=$DIR_EXP/MasterInput.db
DB_CEL=$DIR_EXP/CelsiusV3nov17_dataArise.db
DB_MD=${DATAMILL_WORK}/db/ModelsDictionaryArise.db
echo "DIR_EXP : $DIR_EXP"
echo "DB_MI : $DB_MI"
echo "DB_MD : $DB_MD"
echo "testoption : $testoption"

python3 ${DATAMILL_WORK}/scripts/workflow/run_stics.py --index $i --ncpus $ncpus --testoption $testoption --parts $parts;
wait


