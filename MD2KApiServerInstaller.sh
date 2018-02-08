#!/bin/bash
DB_ENDPOINT=$1
sudo apt-get install python3-pip -y
git clone https://github.com/MD2Korg/CerebralCortex
cd CerebralCortex
git checkout 50c34ae03dc99d8b9931173819002db5c3432c05
pip3 install -r requirements.txt
sudo python3 setup.py install
cd ..
pip3 install -r requirements.txt
sed -ri 's/^(\s*)(host\s*:\s*xxxx\s*$)/\1host: '"$DB_ENDPOINT"' /' ./cc_config_file/cc_configuration.yml
cd app
sudo python3 main.py -c ../cc_config_file/cc_configuration.yml -od /data -mf /merebrum_config/mperf.zip
