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

import json

from flask import Response
from flask import request
from flask_restplus import Namespace, Resource

from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from .. import CC, apiserver_config, influxdb_client
from ..core.data_models import error_model, stream_put_resp, stream_register_model, stream_upload_model
from ..core.decorators import auth_required
from ..util.store_data import store_data, get_data

stream_route = apiserver_config['routes']['stream']
stream_api = Namespace(stream_route, description='Data and annotation streams')


@stream_api.route('/register')
class Stream(Resource):
    @auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put Stream Data')
    @stream_api.expect(stream_register_model(stream_api), validate=True)
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def post(self):
        '''Put Zipped Stream Data'''

        try:
            metadata = request.get_json()
            if isinstance(metadata, str):
                metadata = json.loads(request.form["metadata"])

            metadata = Metadata().from_json_file(metadata=metadata)
            metadata_hash = metadata.get_hash()
            if not metadata.is_valid():
                return {"message": "metadata is not valid."}
        except Exception as e:
            return {"message": "Error in metadata field -> " + str(e)}, 400

        try:
            status = CC.SqlData.save_stream_metadata(metadata)
            if status.get("record_type", "")=="new":
                return {"message": "stream is registered successfully.", "hash_id": str(metadata_hash)}, 200
            else:
                return {"message": "stream is already registered.", "hash_id": str(metadata_hash)}, 200

        except Exception as e:
            return {"message": "Error in registering a new stream -> " + str(e)}, 400


@stream_api.route('/<metadata_hash>')
class Stream(Resource):
    @auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put Stream Data')
    @stream_api.expect(stream_upload_model(stream_api))
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def put(self, metadata_hash):
        '''Put Stream Data'''

        allowed_extensions = set(["gz"])


        try:
            auth_token = request.headers['Authorization']
            auth_token = auth_token.replace("Bearer ", "")

            if len(request.files) == 0:
                return {"message": "File field cannot be empty."}, 400

            file = request.files['file']

            filename = file.filename
            if '.' not in filename and filename.rsplit('.', 1)[1] not in allowed_extensions:
                return {"message": "Uploaded file is not gz."}, 400

            try:
                status = store_data(metadata_hash, auth_token=auth_token, file=file)
            except Exception as e:
                return {"message": "Error in storing data file -> " + str(e)}, 400

            if status.get("status", False):
                output_file = status.get("output_file", "")
                print(output_file)
                message = {'filename': output_file}

                CC.kafka_produce_message("filequeue", message)
                return {"message": status.get("message", "no-messsage-available")}, 200
            else:
                return {"message": status.get("message", "no-messsage-available")}, 400

        except Exception as e:
            return {"message": "Error in file upload and/or publish message on kafka " + str(e)}, 400

@stream_api.route('/<stream_name>/<version>')
class Stream(Resource):
    @auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Get Stream Data')
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def get(self, stream_name, version):
        '''TODO: Get Stream Data'''

        auth_token = request.headers['Authorization']
        auth_token = auth_token.replace("Bearer ", "")
        metadata = []
        try:
            data = get_data(stream_name,auth_token, version=version)

            return Response(data, mimetype=object.getheader("content-type"))

        except Exception as e:
            return {"message": "Error getting data -> " + str(e)}, 400
