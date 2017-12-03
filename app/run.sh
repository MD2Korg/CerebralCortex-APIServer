#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# add current project root directory to pythonpath
export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex-2.0/"


# path of cc configuration path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/cerebralcortex/core/resources/cc_configuration.yml"
# data directory where all gz and json files will be stored
DATA_DIR="/home/ali/IdeaProjects/MD2K_DATA/CC_APISERVER_DATA/"


python3 main.py -c $CC_CONFIG_FILEPATH -od $DATA_DIR