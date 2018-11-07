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
from sys import getsizeof
import os
import pickle
import io
from deepdiff import DeepDiff
from flask import request
from flask_restplus import Namespace, Resource
from datetime import datetime
from .. import CC,apiserver_config
from ..core.data_models import error_model, stream_put_resp, zipstream_data_model
from ..core.decorators import auth_required
from ..core.default_metadata import default_metadata

stream_route = apiserver_config['routes']['stream']
stream_api_aws_s3 = Namespace(stream_route, description='Data and annotation streams')

default_metadata = default_metadata()

@stream_api_aws_s3.route('/s3/zip/')
class Stream(Resource):
    @auth_required
    @stream_api_aws_s3.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api_aws_s3.doc('Put Zipped Stream Data')
    @stream_api_aws_s3.expect(zipstream_data_model(stream_api_aws_s3))
    @stream_api_aws_s3.response(401, 'Invalid credentials.', model=error_model(stream_api_aws_s3))
    @stream_api_aws_s3.response(400, 'Invalid data.', model=error_model(stream_api_aws_s3))
    @stream_api_aws_s3.response(200, 'Data successfully received.', model=stream_put_resp(stream_api_aws_s3))
    def put(self):
        '''Put Zipped Stream Data'''

        # concatenate day with folder path to store files in their respective days folder

        allowed_extensions = set(["gz", "zip"])

        try:
            if isinstance(request.form["metadata"], str):
                metadata = json.loads(request.form["metadata"])
            else:
                metadata = request.form["metadata"]
        except Exception as e:
            return {"message": "Error in metadata field -> " + str(e)}, 400

        current_day = str(datetime.now().strftime("%Y%m%d"))

        try:
            metadata_diff = DeepDiff(default_metadata, metadata)
            if "dictionary_item_removed" in metadata_diff and len(metadata_diff["dictionary_item_removed"]) > 0:
                return {"message": "Missing: " + str(metadata_diff["dictionary_item_removed"])}, 400

            if len(request.files) == 0:
                return {"message": "File field cannot be empty."}, 400

            file = request.files['file']

            filename = file.filename
            if '.' not in filename and filename.rsplit('.', 1)[1] not in allowed_extensions:
                return {"message": "Uploaded file is not gz."}, 400

            file_id = str(uuid.uuid4())
            output_file = file_id + '.gz'
            json_output_file = file_id + '.json.pickle'
            dir_prefix = CC.config['minio']['input_bucket_name']+"/"+CC.config['minio']['dir_prefix']
            output_folder_path = dir_prefix+metadata["owner"]+"/"+current_day+"/" + metadata["identifier"] + "/"

            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            metadata_obj = io.BytesIO(pickle.dumps(metadata))
            metadata_obj.seek(0, os.SEEK_END)
            meta_size = metadata_obj.tell()
            metadata_obj.seek(0)

            CC.upload_object_s3(CC.config['minio']['input_bucket_name'], output_file, file, file_size)
            CC.upload_object_s3(CC.config['minio']['input_bucket_name'], json_output_file, metadata_obj, meta_size)

            return {"message": "Data successfully received."}, 200
        except Exception as e:
            print("Error in creating folder: ", current_day, "Exception: ", str(e))
            return {"message": "Error in creating folder for the day "+str(current_day)}, 400



    def get(self):
        return datetime.now().strftime("%Y%m%d")
