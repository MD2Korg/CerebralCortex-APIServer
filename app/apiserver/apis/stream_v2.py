# Copyright (c) 2017, MD2K Center of Excellence
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

import json
import uuid
import os
from deepdiff import DeepDiff
from flask import request
from flask_restplus import Namespace, Resource
from datetime import datetime
from .. import CC, apiserver_config
from ..core.data_models import error_model, stream_put_resp, zipstream_data_model
from ..core.decorators import auth_required
from ..core.default_metadata import default_metadata
from pprint import pprint
import traceback
import gzip


stream_route = apiserver_config['routes']['stream']
stream_api = Namespace(stream_route, description='Data and annotation streams')

default_metadata = default_metadata()

import pandas
import fastparquet
import msgpack


def convert_to_parquet(input_data):
    result = []

    unpacker = msgpack.Unpacker(input_data, use_list=False, raw=False)
    for unpacked in unpacker:
        read_data = unpacked
        datalist = [d for d in read_data]
        result.append(datalist)

    return result


def create_dataframe(data):
    header = data[0]
    data = data[1:]

    if data is None:
        return None
    else:
        print(data)
        df = pandas.DataFrame(data, columns=header)
        df.Timestamp = pandas.to_datetime(df['Timestamp'], unit='ms')
        df.Timestamp = df.Timestamp.dt.tz_localize('UTC')
        # df.Timestamp = df.Timestamp.dt.tz_convert('America/Chicago')
        # print(df.head())
        return df


def write_parquet(df, file, compressor=None, append=False):
    fastparquet.write(file, df, len(df), compression=compressor, append=append)




@stream_api.route('/')
class Stream(Resource):
    # @auth_required TWH REMOVE THIS
    # @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put MsgPack Gzip Stream Data')
    @stream_api.expect(zipstream_data_model(stream_api))
    # @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def put(self):
        '''Put MsgPacked Gzip Stream Data'''

        allowed_extensions = ("gz")

        try:
            if isinstance(request.form["metadata"], str):
                metadata = json.loads(request.form["metadata"])
            else:
                metadata = request.form["metadata"]
        except Exception as e:
            return {"message": "Error in metadata field -> " + str(e)}, 400

        current_day = str(datetime.now().strftime("%Y%m%d"))

        try:

            version = 1 #TWH: Make this check against mysql and provide the appropriate version
            pprint(metadata)


            #TWH: Make this have an option to write to HDFS
            file_path = os.path.join("stream="+metadata["name"],"version="+str(version),"owner="+metadata["owner"])
            output_folder_path = os.path.join(apiserver_config['data_dir'],'data',file_path)
            json_output_folder_path = os.path.join(apiserver_config['data_dir'],'metadata',file_path)

            file_id = str(current_day + "_" + str(uuid.uuid4()))
            output_file = os.path.join(output_folder_path, file_id + '.parquet')
            json_output_file = os.path.join(json_output_folder_path, file_id + '.json')

            if not os.path.exists(output_folder_path):
                os.makedirs(output_folder_path)
            if not os.path.exists(json_output_folder_path):
                os.makedirs(json_output_folder_path)

            metadata_diff = DeepDiff(default_metadata, metadata)

            if "dictionary_item_removed" in metadata_diff and len(metadata_diff["dictionary_item_removed"]) > 0:
                return {"message": "Missing: " + str(metadata_diff["dictionary_item_removed"])}, 400

            if len(request.files) == 0:
                return {"message": "File field cannot be empty."}, 400

            file = request.files['file']

            filename = file.filename
            if '.' not in filename and filename.rsplit('.', 1)[1] not in allowed_extensions:
                return {"message": "Uploaded file is not gz."}, 400


            with gzip.open(file.stream, 'rb') as input_data:
                data = convert_to_parquet(input_data)
                data_frame = create_dataframe(data)
                write_parquet(data_frame, output_file, compressor='GZIP') # TWH: Make this SNAPPY, edit requirements as well


            #TWH: This should dump to mysql directly after the metadata is rewritten
            with open(json_output_file, 'w') as json_fp:
                json.dump(metadata, json_fp)

            message = {'metadata': metadata,
                       'filename': output_file}

            # CC.kafka_produce_message("filequeue", message)

            return {"message": "Data successfully received."}, 200
        except Exception as e:
            print("Error in file upload and/or publish message on kafka" + str(e), current_day)
            print(e)
            traceback.print_exc()
            return {"message": "Error in file upload and/or publish message on kafka" + str(e) +" - Day:"+str(current_day)}, 400


    #
    # def get(self):
    #     return datetime.now().strftime("%Y%m%d")
