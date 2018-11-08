#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

# path of cc configuration path
CC_CONFIG_FILEPATH="/home/ali/IdeaProjects/CerebralCortex-2.0/conf/"

python3 main.py -c $CC_CONFIG_FILEPATH
