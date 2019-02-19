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
import json
import os
import uuid
from datetime import datetime

import fastparquet
import msgpack
import pandas as pd

from .. import CC


def convert_to_parquet(input_data):
    result = []

    unpacker = msgpack.Unpacker(input_data, use_list=False, raw=False)
    for unpacked in unpacker:
        result.append(list(unpacked))

    return result


def create_dataframe(data):
    header = data[0]
    data = data[1:]

    if data is None:
        return None
    else:
        df = pd.DataFrame(data, columns=header)
        df.Timestamp = pd.to_datetime(df['Timestamp'], unit='us')
        df.Timestamp = df.Timestamp.dt.tz_localize('UTC')
        return df


def write_parquet(df, file_path):
    sdf = CC.sparkSession.createDataFrame(df)
    sdf.coalesce(1).write.format('parquet').mode('append').save(file_path)

def store_data(metadata_hash, auth_token, file, file_checksum=None):
    try:
        checksum = get_file_checksum(file.stream)
        # if checksum==file_checksum:
        #     return {"status":False, "output_file":"", "message": "File checksum doesn't match. Incorrect or corrupt file."}

        file.stream.seek(0)
        user_settings = CC.get_user_settings(auth_token=auth_token)
        stream_info = CC.get_stream_info_by_hash(metadata_hash=metadata_hash)

        if not stream_info:
            return {"status":False, "output_file":"", "message": "Metadata hash is invalid and/or no stream exist for this hash."}

        user_id = user_settings.get("user_id", "")
        stream_name = stream_info.get("name", "")
        stream_version = stream_info.get("version", "")

        file_path = os.path.join("stream="+stream_name, "version="+str(stream_version), "user="+str(user_id))

        output_folder_path = os.path.join(CC.config['filesystem']["filesystem_path"], file_path)

        with gzip.open(file.stream, 'rb') as input_data:
            data = convert_to_parquet(input_data)
            data_frame = create_dataframe(data)
            write_parquet(data_frame, output_folder_path)
        return {"status":True, "output_file":output_folder_path, "message":"Data uploaded successfully."}
    except Exception as e:
        raise Exception(e)

def get_data(stream_name,auth_token, version="all", MAX_DATAPOINTS = 200):

    user_settings = CC.get_user_settings(auth_token=auth_token)
    user_id = user_settings.get("user_id", "")

    if not user_id:
        return {"metadata":"", "data":"", "error": "User is not authenticated or user-id is not available."}

    metadata = []
    ds = CC.get_stream(stream_name=stream_name, version=version)
    ds.filter_user(user_id)
    ds.limit(MAX_DATAPOINTS)
    ds.to_pandas()
    msgpk = ds.data.to_msgpack()

    for md in ds.metadata:
        metadata.append(md.to_json())

    # TODO: convert pandas dataframe to msgpack that mcerebram can interpret
    data = {"metadata":json.dumps(metadata), "data":msgpk, "error": ""}
    return data

import hashlib

def get_file_checksum(file):
    data = file.read()
    return hashlib.md5(data).hexdigest()