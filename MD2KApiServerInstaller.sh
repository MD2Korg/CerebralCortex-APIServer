#!/bin/bash
DB_ENDPOINT=$1
DB_USER=$2
DB_PASSWORD=$3
DB_NAME=$4
sudo apt-get install python3-pip -y
sudo apt-get install mysql-client-core-5.7 -y
git clone https://github.com/MD2Korg/CerebralCortex
cd CerebralCortex
pip3 install -r requirements.txt
sudo python3 setup.py install
cd ..
pip3 install -r requirements.txt
mysql -h$DB_ENDPOINT -u$DB_USER -p$DB_PASSWORD $DB_NAME < ./mysql/cerebralcortex_mysql.sql
sed -ri 's/^(\s*)(host\s*:\s*mysql\s*$)/\1host: '"$DB_ENDPOINT"' /' ./cc_config_file/cc_configuration.yml
sed -ri 's/^(\s*)(database\s*:\s*cerebralcortex\s*$)/\1database: '"$DB_NAME"' /' ./cc_config_file/cc_configuration.yml
sed -ri 's/^(\s*)(db_pass\s*:\s*random_root_password\s*$)/\1db_pass: '"$DB_PASSWORD"' /' ./cc_config_file/cc_configuration.yml
sed -ri 's/^(\s*)(db_user\s*:\s*root\s*$)/\1db_user: '"$DB_USER"' /' ./cc_config_file/cc_configuration.yml
cd app
sudo python3 main.py -c ../cc_config_file/cc_configuration.yml -od /data -mf /merebrum_config/mperf.zip
