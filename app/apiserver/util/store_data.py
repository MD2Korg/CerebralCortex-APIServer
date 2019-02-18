import msgpack
import fastparquet
import pandas
import json
import uuid
import gzip
import os
from deepdiff import DeepDiff
from flask import request
from flask_restplus import Namespace, Resource
from datetime import datetime
from .. import CC, apiserver_config
from ..core.data_models import error_model, stream_put_resp, zipstream_data_model
from ..core.decorators import auth_required
from ..core.default_metadata import default_metadata
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata

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
        df = pandas.DataFrame(data, columns=header)
        df.Timestamp = pandas.to_datetime(df['Timestamp'], unit='us')
        df.Timestamp = df.Timestamp.dt.tz_localize('UTC')
        return df


def write_parquet(df, file, compressor=None, append=False):
    fastparquet.write(file, df, len(df), compression=compressor, append=append)

def store_data(metadata_hash, auth_token, file):
    try:
        user_settings = CC.get_user_settings(auth_token=auth_token)
        stream_info = CC.get_stream_info_by_hash(metadata_hash=metadata_hash)

        user_id = user_settings.get("user_id", "")
        stream_name = stream_info.get("name", "")
        stream_version = stream_info.get("version", "")

        file_path = os.path.join("stream="+stream_name, "version="+str(stream_version), "user="+str(user_id))

        output_folder_path = os.path.join(apiserver_config['data_dir'], 'data', file_path)

        current_day = str(datetime.now().strftime("%Y%m%d"))
        file_id = str(current_day + "_" + str(uuid.uuid4()))

        output_file = os.path.join(output_folder_path, file_id + '.parquet')


        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)


        with gzip.open(file.stream, 'rb') as input_data:
            data = convert_to_parquet(input_data)
            data_frame = create_dataframe(data)
            write_parquet(data_frame, output_file, compressor='SNAPPY')
        return True
    except:
        return False
