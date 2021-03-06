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

import os
from flask import send_file
from flask_restx import Namespace, Resource

from .. import CC, apiserver_config, obj_storage
from ..core.data_models import error_model, bucket_list_resp, object_list_resp, object_stats_resp
from ..core.decorators import auth_required

object_route = apiserver_config['routes']['object']
object_api = Namespace(object_route, description='Object(s) Data Storage')


@object_api.route('/<study_name>')
class MinioObjects(Resource):
    @auth_required
    @object_api.header("Authorization", 'Bearer <JWT>', required=True)
    @object_api.response(400, 'Bad/invalid request.', model=error_model(object_api))
    @object_api.response(401, 'Invalid credentials.', model=error_model(object_api))
    @object_api.response(200, 'Success', model=bucket_list_resp(object_api))
    def get(self, study_name):
        '''List all available buckets'''
        bucket_list = obj_storage.get_or_create_instance(study_name=study_name).get_buckets()
        return bucket_list, 200


@object_api.route('/<study_name>/<string:bucket_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket in Minio storage."})
@object_api.response(404, 'The specified bucket does not exist or name is invalid.', model=error_model(object_api))
@object_api.response(200, 'Success', model=object_list_resp(object_api))
class MinioObjects(Resource):
    @auth_required
    @object_api.header("Authorization", 'Bearer <JWT>', required=True)
    def get(self, study_name, bucket_name):
        '''List objects in a buckets'''
        objects_list = obj_storage.get_or_create_instance(study_name=study_name).get_bucket_objects(bucket_name)
        if "error" in objects_list and objects_list["error"] != "":
            return {"message": objects_list["error"]}, 404

        return objects_list, 200


@object_api.route('/stats/<study_name>/<string:bucket_name>/<string:object_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket.", "object_name": "Name of the object."})
@object_api.response(404, 'The specified bucket/object does not exist or name is invalid.',
                     model=error_model(object_api))
@object_api.response(200, 'Success', model=object_stats_resp(object_api))
class MinioObjects(Resource):
    @auth_required
    @object_api.header("Authorization", 'Bearer <JWT>', required=True)
    def get(self, study_name, bucket_name, object_name):
        '''Object properties'''
        objects_stats = obj_storage.get_or_create_instance(study_name=study_name).get_object_stats(bucket_name, object_name)
        if "error" in objects_stats and objects_stats["error"] != "":
            return {"message": objects_stats["error"]}, 404
        return objects_stats, 200


@object_api.route('/<study_name>/<string:bucket_name>/<string:object_name>')
@object_api.doc(params={"bucket_name": "Name of the bucket.", "object_name": "Name of the object."})
@object_api.response(404, 'The specified bucket does not exist or name is invalid.', model=error_model(object_api))
class MinioObjects12(Resource):
    @auth_required
    @object_api.header("Authorization", 'Bearer <JWT>', required=True)
    def get(self, study_name, bucket_name, object_name):
        '''Download an object'''

        object_path = os.path.join(obj_storage.get_or_create_instance(study_name=study_name).config["object_storage"]["object_storage_path"],"study="+study_name, bucket_name, object_name)
        return send_file(object_path, as_attachment=True)