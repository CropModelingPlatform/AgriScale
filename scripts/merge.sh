#!/usr/bin/env bash

set -e

data='/inputData'
DATAMILL_WORK='/package'


# Source the shared function file
source "${DATAMILL_WORK}/scripts/config_parser.sh"

bound=($(get_list_value "bound"))
models=($(get_list_value "models"))
shapefile=$(get_config_value "shapefile")


echo "Bound: ${bound[@]}"
echo "nchunks = $nchunks"
echo "Bound: ${bound[@]}"
echo "ncpus = $ncpus"


python3 ${DATAMILL_WORK}/scripts/netcdf/merge.py --bnd "${bound[@]}" --models "${models[@]}" --shp "${shapefile}" --ncpus $ncpus;



