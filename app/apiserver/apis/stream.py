# Copyright (c) 2017, MD2K Center of Excellence
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

from deepdiff import DeepDiff
from flask import request
from flask_restplus import Namespace, Resource

from .. import CC
from ..core.data_models import stream_data_model, error_model, stream_put_resp, zipstream_data_model
from ..core.decorators import auth_required
from ..core.default_metadata import default_metadata

stream_route = CC.configuration['routes']['stream']
stream_api = Namespace(stream_route, description='Data and annotation streams')

default_metadata = default_metadata()


@stream_api.route('/')
class Stream(Resource):
    # @auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put Stream Data')
    @stream_api.expect(stream_data_model(stream_api), validate=True)
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def put(self):
        '''Put Stream Data'''

        json_object = request.get_json()

        identifier = json_object.get('identifier', None)
        owner = json_object.get('owner', None)
        name = json_object.get('name', None)
        data_descriptor = json_object.get('data_descriptor', None)
        execution_context = json_object.get('execution_context', None)
        annotations = json_object.get('annotations', None)

        # CC.kafka_produce_message("stream", request.json)
        return {"message": "Data successfully received."}, 200


@stream_api.route('/zip/')
class Stream(Resource):
    @auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put Zipped Stream Data')
    @stream_api.expect(zipstream_data_model(stream_api))
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def put(self):
        '''Put Zipped Stream Data'''

        allowed_extensions = set(["gz"])

        metadata = request.form["metadata"]

        try:
            metadata = json.loads(metadata)
        except Exception as e:
            return {"message": "Error in metadata field -> " + str(e)}, 400

        metadata_diff = DeepDiff(default_metadata, metadata)
        if "dictionary_item_removed" in metadata_diff and len(metadata_diff["dictionary_item_removed"]) > 0:
            return {"message": "Missing: " + str(metadata_diff["dictionary_item_removed"])}, 400

        if len(request.files) == 0:
            return {"message": "File field cannot be empty."}, 400

        file = request.files['file']

        filename = file.filename
        if '.' not in filename and filename.rsplit('.', 1)[1] not in allowed_extensions:
            return {"message": "Uploaded file is not gz."}, 400

        output_file = '/data/' + str(uuid.uuid4()) + '.gz'
        with open(output_file, 'wb') as fp:
            file.save(fp)

        message = {'metadata': metadata,
                   'filename': output_file}

        CC.kafka_produce_message("filequeue", message)

        return {"message": "Data successfully received."}, 200
