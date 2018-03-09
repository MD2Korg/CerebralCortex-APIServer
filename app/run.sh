#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# add current project root directory to pythonpath
export PYTHONPATH="${PYTHONPATH}:/home/vagrant/CerebralCortex"


# path of cc configuration path
CC_CONFIG_FILEPATH="/home/vagrant/IdeaProjects/CerebralCortex-DockerCompose/cc_config/cc_vagrant_configuration.yml"
# data directory where all gz and json files will be stored
DATA_DIR="/home/vagrant/IdeaProjects/CerebralCortex-DockerCompose/data"


python3 main.py -c $CC_CONFIG_FILEPATH -od $DATA_DIR
