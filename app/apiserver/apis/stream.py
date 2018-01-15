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
import io
import boto3
import uuid
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
    # below variables should come from OS environment or a properties file.
    __awsKinesisStreamName = 'Md2kKinesisStream'
    __awsKinesisStreamRegionName = 'us-east-1'
    __awsApiStreamBucketName = 'stream-api-bucket'

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
        self.__putFileToS3(output_folder_path + output_file, file.read())

        with open(output_folder_path + json_output_file, 'w') as json_fp:
            json.dump(metadata, json_fp)
        self.__putFileToS3(output_folder_path + json_output_file, metadata)

        message = {'metadata': metadata,
                   'filename': current_day+"/"+output_file}

        #CC.kafka_produce_message("filequeue", message)
        self.__produceMessage(str(message), file_id)

        return {"message": "Data successfully received."}, 200

    def get(self):
        return datetime.now().strftime("%Y%m%d")

    def __putFileToS3(self, s3FolderPath, fileToSave):
        if (s3FolderPath[0] == '/'):
            s3FolderPath = s3FolderPath[1:]     # we don't need the starting folder separator while adding data to S3 as it cause empty folder name

        try:
            s3_resource = boto3.resource(service_name='s3', region_name=self.__awsKinesisStreamRegionName)
            apiStreamS3Bucket = s3_resource.Bucket(self.__awsApiStreamBucketName)

            utc_datetime = datetime.utcnow()
            date_time_str = utc_datetime.strftime("_%Y-%m-%d_%H_%M_%S")
            metadata = {
                'uploaded_datetime': date_time_str
            }

            print("Started uploading contents to TARGET_BUCKET: " + 
                self.__awsApiStreamBucketName + ', FOLDER_PATH:' + s3FolderPath)

            apiStreamS3Bucket.put_object(Body=fileToSave, Key=s3FolderPath, Metadata=metadata)
            print("Successfully uploaded contents to TARGET_BUCKET: " + 
                self.__awsApiStreamBucketName + ', FOLDER_PATH:' + s3FolderPath)

        except Exception as e:
            print('Error uploading data to bucket {}.'.format(self.__awsApiStreamBucketName))
            raise e
        print("Done")

    def __produceMessage(self, streamMessage, partitionKeyFactor):
        if(self.__awsKinesisStreamName == '' or self.__awsKinesisStreamRegionName == ''):
            print('Cannot work with empty awsAccountNumber'
                ' or awsKinesisStreamName value(s).')
            return

        try:
            print('About to create boto client object.')
            kinesisClient = boto3.client('kinesis', self.__awsKinesisStreamRegionName)
            print('Succesfully created boto client object. About to put records on stream.')
            #print('Stream :' + self.__awsKinesisStreamName)
            #print('Data :' + json.dumps(streamMessage))
            #print('PartitionKey :' + str(hash(partitionKeyFactor)))

            kinesisClient.put_record(StreamName=self.__awsKinesisStreamName, 
                Data=streamMessage,
                PartitionKey=str(hash(partitionKeyFactor)))

            print("Successfully sent message :" + streamMessage + " to stream :" + self.__awsKinesisStreamName)

        except Exception as e:
            print('Received exception :' + str(e))

