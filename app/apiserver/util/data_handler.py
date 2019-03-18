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

import gzip
import hashlib
import json
import os
import warnings
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.util.data_formats import msgpack_to_pandas
from .. import CC, influxdb_client, data_ingestion_config, cc_config

# Disable pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def write_to_influxdb(user_id: str, username: str, stream_name: str, df: pd.DataFrame):
    """
    Store data in influxdb. Influxdb is used for visualization purposes

    Args:
        user_id (str): id of a user
        username (str): username
        stream_name (str): name of a stream
        df (pandas): pandas dataframe

    Raises:
        Exception: if error occurs during storing data to influxdb
    """
    ingest_influxdb = data_ingestion_config["data_ingestion"]["influxdb_in"]

    influxdb_blacklist = data_ingestion_config["influxdb_blacklist"]
    if ingest_influxdb and stream_name not in influxdb_blacklist.values():
        try:
            df["stream_name"] = stream_name
            df["user_id"] = user_id
            df['username'] = username

            tags = ['username', 'user_id', 'stream_name']
            df.set_index('Timestamp', inplace=True)
            influxdb_client.write_points(df, measurement=stream_name, tag_columns=tags, protocol='json')

            df.drop("stream_name", 1)
            df.drop("user_id", 1)
            df.drop("username", 1)
        except Exception as e:
            raise Exception("Error in writing data to influxdb. " + str(e))


def write_to_nosql(df: pd, user_id: str, stream_name: str) -> str:
    """
    Store data in a selected nosql database (e.g., filesystem, hdfs)

    Args:
        df (pandas): pandas dataframe
        user_id (str): user id
        stream_name (str): name of a stream

    Returns:
        str: file_name of newly create parquet file

    Raises:
         Exception: if selected nosql database is not implemented

    """
    ingest_nosql = data_ingestion_config["data_ingestion"]["nosql_in"]
    if ingest_nosql:
        table = pa.Table.from_pandas(df, preserve_index=False)

        file_id = str(uuid4().hex) + ".parquet"

        if cc_config["nosql_storage"] == "filesystem":
            base_dir_path = cc_config["filesystem"]["filesystem_path"]
            data_file_url = os.path.join(base_dir_path, "stream=" + stream_name, "version=1", "user=" + user_id)
            file_name = os.path.join(data_file_url, file_id)
            if not os.path.exists(data_file_url):
                os.makedirs(data_file_url)

            pq.write_table(table, file_name)

        elif cc_config["nosql_storage"] == "hdfs":
            base_dir_path = cc_config["hdfs"]["raw_files_dir"]
            data_file_url = os.path.join(base_dir_path, "stream=" + stream_name, "version=1", "user=" + user_id)
            file_name = os.path.join(data_file_url, file_id)
            fs = pa.hdfs.connect(cc_config['hdfs']['host'], cc_config['hdfs']['port'])
            if not fs.exists(data_file_url):
                fs.mkdir(data_file_url)
            with fs.open(file_name, "wb") as fp:
                pq.write_table(table, fp)
        else:
            raise Exception(str(cc_config["nosql_storage"]) + " is not supported. Please use filesystem or hdfs.")
        return file_name.replace(base_dir_path, "")


def store_data(stream_info: str, user_settings: str, file: object, file_checksum=None):
    """
    Store data in influxdb and/or nosql storage (e.g., filesystem, hdfs)

    Args:
        stream_info (str): hash value of a stream
        user_settings (str): java web token string
        file (object): object of a file
        file_checksum (str): checksum of a file

    Returns:
        dict: {"status":bool, "output_file":str, "message":str}

    Raises:
        Exception: if error occurs during processing file and/or storing data to databases

    Todo:
        Confirm which checksum mcerebrum is going to prdocue then write equivalent code in pythong to verify file checksum
    """
    try:
        checksum = get_file_checksum(file.stream)
        # if checksum==file_checksum:
        #     return {"status":False, "output_file":"", "message": "File checksum doesn't match. Incorrect or corrupt file."}
        parquet_file_name = None
        file.stream.seek(0)

        if not stream_info:
            return {"status": False, "output_file": "",
                    "message": "Metadata hash is invalid and/or no stream exist for this hash."}

        user_id = user_settings.get("user_id", "")
        username = user_settings.get("username", "")
        stream_name = stream_info.get("name", "")
        stream_version = stream_info.get("version", "")

        file_path = os.path.join("stream=" + stream_name, "version=" + str(stream_version), "user=" + str(user_id))

        # output_folder_path = os.path.join(CC.config['filesystem']["filesystem_path"], file_path)

        with gzip.open(file.stream, 'rb') as input_data:
            data_frame = msgpack_to_pandas(input_data)
            parquet_file_name = write_to_nosql(data_frame, user_id, stream_name)
            write_to_influxdb(user_id, username, stream_name, data_frame)

        return {"status": True, "output_file": parquet_file_name, "message": "Data uploaded successfully."}
    except Exception as e:
        raise Exception(e)


def get_data(auth_token: str, stream_name: str, version: str = "all", MAX_DATAPOINTS: int = 200):
    """
    Get data back from CerebralCortex-Kerenel.

    Args:
        stream_name (str): name of a stream
        auth_token (str): java web token
        version (str): version of a stream. default is to return all versions of a stream
        MAX_DATAPOINTS (int): max number of datapoints that should be return to a user

    Returns:
        object
    """

    user_settings = CC.get_user_settings(auth_token=auth_token)
    user_id = user_settings.get("user_id", "")

    if not user_id:
        return {"error": "User is not authenticated or user-id is not available."}

    ds = CC.get_stream(stream_name=stream_name)
    ds.filter_user(user_id)
    if version is not None and version != "all":
        ds.filter_version(version=version)
    ds.limit(MAX_DATAPOINTS)
    ds.drop_column(*["user", "version"])
    pdf = ds.to_pandas()

    # TODO: convert pandas dataframe to msgpack that mcerebram can interpret
    # msgpk = pandas_to_msgpack(pdf)

    return pdf


def get_data_metadata(auth_token: str, stream_name: str, version: str = "all"):
    """
    Get data back from CerebralCortex-Kerenel.

    Args:
        stream_name (str): name of a stream
        auth_token (str): java web token
        version (str): version of a stream. default is to return all versions of a stream

    Returns:
        list(dict): list of metadata objects
    """

    user_settings = CC.get_user_settings(auth_token=auth_token)
    user_id = user_settings.get("user_id", "")

    if not user_id:
        return {"metadata": "", "data": "", "error": "User is not authenticated or user-id is not available."}

    metadata_lst = []
    ds = CC.get_stream(stream_name=stream_name, data_type=DataSet.ONLY_METADATA)

    metadata = ds.metadata
    for md in metadata:
        if version != "all":
            metadata_lst.append(md.to_json())
        elif int(md.version) == int(version):
            metadata_lst.append(md.to_json)

    data = {"metadata": json.dumps(metadata_lst)}
    return data


def get_file_checksum(file: object):
    """
    generate checksum of a file

    Args:
        file (object): file object

    Returns:
        md5 checksum
    """
    data = file.read()
    return hashlib.md5(data).hexdigest()