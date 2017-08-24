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

import gzip

from flask import request
from flask_restplus import Namespace, Resource

import json
import datetime

from .. import CC
from ..core.data_models import stream_data_model, error_model, stream_put_resp
from ..core.decorators import auth_required

stream_route = CC.configuration['routes']['stream']
stream_api = Namespace(stream_route, description='Data and annotation streams')

def chunks(data, max_len):
    """Yields max_len sized chunks with the remainder in the last"""
    for i in range(0, len(data), max_len):
        yield data[i:i+max_len]

def datapoint(row):
    """
    Format data based on mCerebrum's current GZ-CSV format into what Cerebral
    Cortex expects
    """
    ts,offset,values = row.split(',',2)
    ts = int(ts)/1000.0
    offset = int(offset)
    values = list(map(float, values.split(',')))

    timezone = datetime.timezone(datetime.timedelta(milliseconds=offset))
    ts = datetime.datetime.fromtimestamp(ts,timezone)

    return {'starttime': str(ts), 'value': values}



@stream_api.route('/')
class Stream(Resource):
    @auth_required
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

        CC.kafka_produce_message("stream", request.json)
        return {"message": "Data successfully received."}, 200


@stream_api.route('/zip/')
class Stream(Resource):
    #@auth_required
    @stream_api.header("Authorization", 'Bearer <JWT>', required=True)
    @stream_api.doc('Put Zipped Stream Data')
    @stream_api.doc(params={'file': {'in': 'formData', 'description': 'Resource name'}})
    @stream_api.param('file', description='Zipped data stream', _in='formData', type='file', required=True)
    @stream_api.response(401, 'Invalid credentials.', model=error_model(stream_api))
    @stream_api.response(400, 'Invalid data.', model=error_model(stream_api))
    @stream_api.response(200, 'Data successfully received.', model=stream_put_resp(stream_api))
    def put(self):
        '''Put Zipped Stream Data'''

        allowed_extensions = set(["gz"])

        file = request.files['file']
        filename = file.filename
        if '.' not in filename and filename.rsplit('.', 1)[1] not in allowed_extensions:
            return {"message": "Uploaded file is not gz."}, 400

        gzip_file = gzip.open(file, 'rb')
        gzip_file_content = gzip_file.read()
        gzip_file_content = gzip_file_content.decode('utf-8')


        metadata_header = {'identifier': 'asdfkjladfaldf'}

        lines = list(map(lambda x: datapoint(x), gzip_file_content.splitlines()))
        for d in chunks(lines, 1000):
            json_object = {'metadata': metadata_header, 'data': d}
            # print(len(d), metadata_header)
            CC.kafka_produce_message("stream", json.dumps(json_object))

        return {"message": "Data successfully received."}, 200
