from influxdb import DataFrameClient


def get_influxdb_client(cc_config):
    if cc_config["visualization_storage"]=="none":
        raise Exception("Visualization storage is disabled (none) in cerebralcortex.yml. Please update configs.")

    user = cc_config["influxdb"]["db_user"]
    password = cc_config["influxdb"]["db_pass"]
    dbname = cc_config["influxdb"]["database"]
    db_host = cc_config["influxdb"]["host"]
    db_port = cc_config["influxdb"]["port"]

    return DataFrameClient(db_host, db_port, user, password, dbname)
