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
import os
import uuid
import json
import warnings
from datetime import datetime
import pandas as pd
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.core.util.data_formats import msgpack_to_pandas
from .. import CC, data_ingestion_config, cc_config

# Disable pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def get_file_data(file_format, file_data):
    if file_format == "msgpack":
        data_frame = msgpack_to_pandas(file_data)
    elif file_format == "csv":
        data_frame = pd.read_csv(file_data)
    else:
        raise Exception("File format not supported.")

    return data_frame

def store_data(stream_info: dict, user_settings: str, file: object, study_name, file_checksum=None, file_format="msgpack"):
    """
    Store data in influxdb and/or nosql storage (e.g., filesystem, hdfs)

    Args:
        stream_info (dict): stream metadata
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
        generated_file_name = None
        file.stream.seek(0)

        if not stream_info:
            return {"status": False, "output_file": "",
                    "message": "Metadata hash is invalid and/or no stream exist for this hash."}

        user_id = user_settings.get("user_id", "")
        username = user_settings.get("username", "")
        stream_name = stream_info.get("name", "")
        stream_version = stream_info.get("version", "")

        data_descriptor = json.loads(stream_info.get("metadata",'{}')).get("data_descriptor", '{}')
        total_columns_in_metadata = len(data_descriptor)
        # file_path = os.path.join("stream=" + stream_name, "version=" + str(stream_version), "user=" + str(user_id))

        # output_folder_path = os.path.join(CC.config['filesystem']["filesystem_path"], file_path)

        from .. import CC, data_ingestion_config, cc_config

        ingestion_type = data_ingestion_config["data_ingestion"]["ingestion_type"]

        if ingestion_type=="offline":
            raw_data_path = data_ingestion_config["data_ingestion"]["raw_data_path"]
            if raw_data_path[-1:] != "/":
                raw_data_path = raw_data_path + "/"

            if not os.path.exists(raw_data_path):
                return {"status": False, "message": "Please check data_ingestion.yml file. parameters are not set properly."+str(raw_data_path)+" - does not exist."}

            try:
                day = str(datetime.now().strftime('%m%d%Y'))
                hour = str(datetime.now().strftime('%H'))
                output_folder_path = raw_data_path+"study="+study_name+"/stream="+stream_name+"/version="+str(stream_version)+"/user="+user_id+"/"+day+"/"+hour+"/"
                if not os.path.exists(output_folder_path):
                    os.makedirs(output_folder_path)

                file_id = str(uuid.uuid4())
                output_file = file_id + '.gz'

                with open(output_folder_path + output_file, 'wb') as fp:
                    file.save(fp)

                return {"status": True, "output_file": output_file, "message": "Data uploaded successfully."}
            except Exception as e:
                return {"status": False,
                        "message": "Cannot store data in offline mode. "+str(e)}

        elif ingestion_type=="online":
            try:
                with gzip.open(file.stream, 'rb') as input_data:
                    data_frame = get_file_data(file_format, input_data)
            except:
                file.stream.seek(0)
                data_frame = get_file_data(file_format, file)

            if total_columns_in_metadata!=len(data_frame.columns):
                return {"status": False, "message": "Number of column mismatch for stream"+stream_name+" - Metadata contains total "+str(total_columns_in_metadata)+ " columns and data contains total "+str(len(data_frame.columns))+" number of colummns."}

            if set(data_frame.columns)!=set([d['name'] for d in data_descriptor]):
                return {"status": False, "message": "Column names mismatch. Data Columns: ["+ ','.join(data_frame.columns)+"] - Metadata Columns: ["+','.join([d['name'] for d in data_descriptor])+"]"}

            generated_file_name = CC.get_or_create_instance(
                study_name=study_name).RawData.nosql.write_pandas_to_parquet_file(data_frame, user_id, stream_name, stream_version)

            if cc_config["visualization_storage"]!="none" and stream_name not in list(data_ingestion_config["influxdb_blacklist"].values()):
                CC.get_or_create_instance(study_name=study_name).TimeSeriesData.write_pd_to_influxdb(user_id, username, stream_name, data_frame)

            return {"status": True, "output_file": generated_file_name, "message": "Data uploaded successfully."}
        else:
            return {"status": False, "message": "Please check data_ingestion.yml file. parameters are not set properly."}
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
