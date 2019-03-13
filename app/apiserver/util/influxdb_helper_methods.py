from influxdb import DataFrameClient

def get_influxdb_client(CC_config):
    user = ''
    password = ''
    dbname = 'cerebralcortex_raw'
    protocol = 'json'
    client = DataFrameClient("127.0.0.1", 8086, user, password, dbname)