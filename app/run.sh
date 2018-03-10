#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# add current project root directory to pythonpath

# path of cc configuration path
CC_CONFIG_FILEPATH="/cc_config/cc_vagrant_configuration.yml"
# data directory where all gz and json files will be stored
DATA_DIR="/data"


python3 main.py -c $CC_CONFIG_FILEPATH -od $DATA_DIR -mf /merebrum_config
