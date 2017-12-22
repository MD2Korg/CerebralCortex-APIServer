# Copyright (c) 2017, MD2K Center of Excellence
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

from cerebralcortex.cerebralcortex import CerebralCortex

parser = argparse.ArgumentParser(description='CerebralCortex API Server.')
parser.add_argument("-c", "--config_filepath", help="Configuration file path", required=True)
parser.add_argument("-od", "--output_data_dir",
                    help="Directory path where all the gz files will be stored by API-Server",
                    required=True)
parser.add_argument("-mf", "--mcerebrum_config",
                    help="mCerebrum Configuration file.",
                    required=True)

args = vars(parser.parse_args())

if not args['config_filepath'] or not args['output_data_dir']:
    raise ValueError("Missing command line args.")

CC = CerebralCortex(args['config_filepath'])

CC.config["output_data_dir"] = str(args['output_data_dir']).strip()

if (CC.config['output_data_dir'][-1] != '/'):
    CC.config['output_data_dir'] += '/'

CC.config["mcerebrum_config"] = str(args['mcerebrum_config']).strip()
