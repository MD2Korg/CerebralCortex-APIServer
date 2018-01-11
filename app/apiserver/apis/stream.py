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
from .. import CC
from ..core.data_models import error_model, stream_put_resp, zipstream_data_model
from ..core.decorators import auth_required
from ..core.default_metadata import default_metadata

stream_route = CC.config['routes']['stream']
stream_api = Namespace(stream_route, description='Data and annotation streams')

default_metadata = default_metadata()


output_folder_path = CC.config['output_data_dir']
if (output_folder_path[-1] != '/'):
    output_folder_path += '/'
# concatenate day with folder path to store files in their respective days folder
current_day = str(datetime.now().strftime("%Y%m%d"))
output_folder_path = output_folder_path+current_day+"/"

if not os.path.exists(output_folder_path):
    os.makedirs(output_folder_path)

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

        allowed_extensions = set(["gz", "zip"])

        try:
            if isinstance(request.form["metadata"], str):
                metadata = json.loads(request.form["metadata"])
            else:
                metadata = request.form["metadata"]
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

        file_id = str(uuid.uuid4())
        output_file = file_id + '.gz'
        json_output_file = file_id + '.json'

        with open(output_folder_path + output_file, 'wb') as fp:
            file.save(fp)

        with open(output_folder_path + json_output_file, 'w') as json_fp:
            json.dump(metadata, json_fp)

        message = {'metadata': metadata,
                   'filename': current_day+"/"+output_file}

        #CC.kafka_produce_message("filequeue", message)
        self.produceMessage(message, file_id)

        return {"message": "Data successfully received."}, 200

    def get(self):
        return datetime.now().strftime("%Y%m%d")

    def __produceMessage(self, streamMessage, partitionKeyFactor):

        # check for the required variables.
        # awsAccountNumber = os.getenv(EnvironVariableConstants.AWS_ACCOUNT_NUMBER, '')
        # awsKinesisStreamName = os.getenv(EnvironVariableConstants.AWS_KINESIS_STREAM_NAME, '')
        # awsKinesisStreamRegionName = os.getenv(EnvironVariableConstants.AWS_KINESIS_STREAM_REGION, '')

        # below variables should come from OS environment or a properties file.
        awsAccountNumber = '376137242575'
        awsKinesisStreamName = 'Md2kKinesisStream'
        awsKinesisStreamRegionName = 'us-east-1'

        if(awsAccountNumber == '' or awsKinesisStreamName == '' or awsKinesisStreamRegionName == ''):
            print('Cannot work with empty awsAccountNumber'
                ' or awsKinesisStreamName value(s).')
            return

        try:
            kinesisClient = boto3.client('kinesis', awsKinesisStreamRegionName)

            kinesisClient.put_record(StreamName=awsKinesisStreamName, 
                Data=json.dumps(streamMessage),
                PartitionKey=str(hash(partitionKeyFactor)))

            print("Successfully sent message :" + streamMessage + " to stream :" + awsKinesisStreamName)

        except Exception as e:
            print('Received exception :' + str(e))