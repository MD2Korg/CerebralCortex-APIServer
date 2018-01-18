#!/bin/bash
curl -sL https://repos.influxdata.com/influxdb.key | sudo apt-key add -
source /etc/lsb-release
echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
sudo apt-get update
sudo apt-get install influxdb
sudo service influxdb start
curl "http://localhost:8086/query" --data-urlencode "q=DROP DATABASE cerebralcortex_raw"
curl "http://localhost:8086/query" --data-urlencode "q=DROP DATABASE cerebralcortex_1hour"
curl "http://localhost:8086/query" --data-urlencode "q=DROP DATABASE cerebralcortex_1minute"
curl "http://localhost:8086/query" --data-urlencode "q=CREATE DATABASE cerebralcortex_raw WITH DURATION 7d REPLICATION 1"
curl "http://localhost:8086/query" --data-urlencode "q=CREATE DATABASE cerebralcortex_1hour"
curl "http://localhost:8086/query" --data-urlencode "q=CREATE DATABASE cerebralcortex_1minute"
curl "http://localhost:8086/query" --data-urlencode "q=USE cerebralcortex_raw"
curl "http://localhost:8086/query" --data-urlencode "q=CREATE CONTINUOUS QUERY "cq_1min" on "cerebralcortex_raw" RESAMPLE EVERY 15m FOR 2h BEGIN SELECT count(*::field) AS "count", mean(*::field) AS "mean" INTO "cerebralcortex_1minute"."autogen".:MEASUREMENT FROM /.*/ GROUP BY time(1m), owner_id, owner_name, stream_id END"
curl "http://localhost:8086/query" --data-urlencode "q=CREATE CONTINUOUS QUERY "cq_1hour" on "cerebralcortex_raw" RESAMPLE EVERY 1h FOR 6h BEGIN SELECT count(*::field) AS "count", mean(*::field) as "mean" INTO "cerebralcortex_1hour"."autogen".:MEASUREMENT FROM /.*/ GROUP BY time(1h), owner_id, owner_name, stream_id END"
