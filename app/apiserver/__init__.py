# Copyright (c) 2019, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse

from cerebralcortex import Kernel
# from apiserver.util.influxdb_helper_methods import get_influxdb_client
from apiserver.util.cc_kernels_hashmap import CCKernelHashMap
from cerebralcortex.core.config_manager.config import Configuration

parser = argparse.ArgumentParser(description='CerebralCortex API Server.')
parser.add_argument("-c", "--config_filepath", help="Configuration directory path", required=True)

args = vars(parser.parse_args())

config_dir_path = args['config_filepath']

#CC = Kernel(configs_dir_path=config_dir_path, enable_spark=True, study_name="default")
CC = CCKernelHashMap(configs_dir_path=config_dir_path)
cc_config = CC.config
apiserver_config = Configuration(config_dir_path, "api_server.yml").config
data_ingestion_config = Configuration(config_dir_path, "data_ingestion.yml").config

#influxdb_client = get_influxdb_client(cc_config)
