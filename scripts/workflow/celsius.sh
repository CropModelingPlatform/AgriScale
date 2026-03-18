#!/usr/bin/env bash

export INDEXES=$INDEXES #($SLURM_ARRAY_TASK_ID)
export ncpus=$ncpus # number of cpus

testoption=$1
parts=$2

if [[ -z "$DATAMILL_WORK" ]]; then
  export DATAMILL_WORK='/package'
fi

echo "celsius INDEXES : $INDEXES"
echo "testoption : $testoption"
i=$INDEXES;

work_dir='/inter'
DIR_EXP=${work_dir}/EXPS/exp_$i

cd $DIR_EXP

python3 ${DATAMILL_WORK}/scripts/workflow/run_celsius.py --index $i --ncpus $ncpus --testoption $testoption --parts $parts;
wait


