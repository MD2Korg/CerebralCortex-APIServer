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

from apiserver import CC
from apiserver.core.decorators import auth_required
from cerebralcortex.kernel.DataStoreEngine.Data.minio_storage import MinioStorage
from apiserver.core.data_models import auth_data_model
from flask import request
import json
import flask
from flask import send_file, Response
from flask_jwt_extended import create_access_token, get_jwt_identity, jwt_required, get_raw_jwt, decode_token
from flask_restplus import Namespace, Resource
from datetime import datetime, timedelta


object_route = CC.configuration['routes']['object']
object_api = Namespace(object_route, description='Object(s) Data Storage')

@object_api.route('/')
class MinioObjects(Resource):
    #@auth_required
    #@object_api.header("Authorization", 'Bearer JWT', required=True)
    def get(self):
        '''List all available buckets'''
        bucket_list = MinioStorage(CC).list_buckets()
        return bucket_list, 200

@object_api.route('/<string:bucket_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket in Minio storage."})
@object_api.response(404, 'The specified bucket does not exist or name is invalid.')
class MinioObjects(Resource):
    #@auth_required
    #@object_api.header("Authorization", 'Bearer JWT', required=True)
    def get(self, bucket_name): #TODO: bucketname cannot be less than 3 char
        '''List objects in a buckets'''
        objects_list = MinioStorage(CC).list_objects_in_bucket(bucket_name)
        if "error" in objects_list and objects_list["error"]!="":
            return objects_list["error"], 404

        print(objects_list)

        return objects_list, 200

@object_api.route('/stats/<string:bucket_name>/<string:object_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket.", "object_name": "Name of the object."})
@object_api.response(404, 'The specified bucket/object does not exist or name is invalid.')
class MinioObjects(Resource):
    #@auth_required
    #@object_api.header("Authorization", 'Bearer JWT', required=True)
    def get(self, bucket_name, object_name): #TODO: bucketname cannot be less than 3 char
        '''Object properties'''
        objects_stats = MinioStorage(CC).get_object_stat(bucket_name, object_name)
        if "error" in objects_stats and objects_stats["error"]!="":
            return objects_stats["error"], 404
        return json.loads(objects_stats), 200

@object_api.route('/<string:bucket_name>/<string:object_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket.", "object_name": "Name of the object."})
@object_api.response(404, 'The specified bucket does not exist or name is invalid.')
class MinioObjects12(Resource):
    #@auth_required
    #@object_api.header("Authorization", 'Bearer JWT', required=True)
    #@object_api.marshal_with(auth_data_model(object_api), envelope='resource')
    def get(self, bucket_name, object_name): #TODO: bucketname cannot be less than 3 char
        '''Download an object'''
        object = MinioStorage(CC).get_object(bucket_name, object_name)

        if type(object) is dict and "error" in object and object["error"]!="":
            return object["error"], 404

        return Response(object.data, mimetype=object.getheader("content-type"))