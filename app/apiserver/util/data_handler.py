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
import warnings

from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.util.data_formats import msgpack_to_pandas
from .. import CC, data_ingestion_config

# Disable pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def store_data(stream_info: str, user_settings: str, file: object, study_name, file_checksum=None):
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

        # file_path = os.path.join("stream=" + stream_name, "version=" + str(stream_version), "user=" + str(user_id))

        # output_folder_path = os.path.join(CC.config['filesystem']["filesystem_path"], file_path)
        from .. import CC, apiserver_config
        with gzip.open(file.stream, 'rb') as input_data:
            data_frame = msgpack_to_pandas(input_data)
            parquet_file_name = CC.get_or_create_instance(study_name=study_name).RawData.nosql.write_pandas_to_parquet_file(data_frame, user_id, stream_name)
            if stream_name not in list(data_ingestion_config["influxdb_blacklist"].values()):
                CC.get_or_create_instance(study_name=study_name).TimeSeriesData.write_pd_to_influxdb(user_id, username, stream_name, data_frame)

        return {"status": True, "output_file": parquet_file_name, "message": "Data uploaded successfully."}
    except Exception as e:
        raise Exception(e)


def get_data(auth_token: str, study_name:str, stream_name: str, version: str = "all", MAX_DATAPOINTS: int = 200):
    """
    Get data back from CerebralCortex-Kerenel.

    Args:
        auth_token (str): java web token
        study_name (str): study name
        stream_name (str): name of a stream
        version (str): version of a stream. default is to return all versions of a stream
        MAX_DATAPOINTS (int): max number of datapoints that should be return to a user

    Returns:
        object
    """

    user_settings = CC.get_or_create_instance(study_name=study_name).get_user_settings(auth_token=auth_token)
    user_id = user_settings.get("user_id", "")

    if not user_id:
        return {"error": "User is not authenticated or user-id is not available."}

    ds = CC.get_or_create_instance(study_name=study_name).get_stream(stream_name=stream_name)
    ds.filter_user(user_id)
    if version is not None and version != "all":
        ds.filter_version(version=version)
    ds.limit(MAX_DATAPOINTS)
    ds.drop_column(*["user", "version"])
    pdf = ds.to_pandas()

    # TODO: convert pandas dataframe to msgpack that mcerebram can interpret
    # msgpk = pandas_to_msgpack(pdf)

    return pdf


def get_metadata(auth_token: str, study_name:str, stream_name: str, version: str = "all"):
    """
    Get data back from CerebralCortex-Kerenel.

    Args:
        auth_token (str): java web token
        study_name (str): study name
        stream_name (str): name of a stream
        version (str): version of a stream. default is to return all versions of a stream

    Returns:
        list(dict): list of metadata objects
    """

    user_settings = CC.get_or_create_instance(study_name=study_name).get_user_settings(auth_token=auth_token)
    user_id = user_settings.get("user_id", "")

    if not user_id:
        return {"metadata": "", "data": "", "error": "User is not authenticated or user-id is not available."}

    metadata_lst = []
    ds = CC.get_or_create_instance(study_name=study_name).get_stream(stream_name=stream_name, data_type=DataSet.ONLY_METADATA)

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
