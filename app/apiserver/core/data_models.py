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

from flask_restx import fields as rest_fields
from flask_restx import Namespace

################################################################
#                     Request Models                           #
################################################################

def stream_upload_model(stream_api:Namespace):
    request_parser = stream_api.parser()
    request_parser.add_argument('file', location='files',
                                type='file', required=True)
    return request_parser

def stream_register_model(stream_api:Namespace):
    attributes = stream_api.model('Attributes', {
        'key': rest_fields.String(required=False),
        'value': rest_fields.String(required=False)
    })

    data_descriptor = stream_api.model('DataDescriptor', {
        'name': rest_fields.String(required=True),
        'type': rest_fields.String(required=True),
        'attributes': rest_fields.Nested(attributes, required=False)
    })
    authors = stream_api.model('author', {
        'name': rest_fields.String(required=False),
        'email': rest_fields.String(required=False),
        'attributes': rest_fields.Nested(attributes, required=False)
    })
    modules = stream_api.model('Modules', {
        'name': rest_fields.String(required=True),
        'version': rest_fields.String(required=True),
        'authors': rest_fields.List(rest_fields.Nested(authors), required=True),
        'attributes': rest_fields.Nested(attributes, required=False)
    })

    stream = stream_api.model('Stream', {
        'name': rest_fields.String(required=True),
        'description': rest_fields.String(required=True),
        'data_descriptor': rest_fields.List(rest_fields.Nested(data_descriptor), required=False),
        'modules': rest_fields.List(rest_fields.Nested(modules), required=True)
    })

    return stream


def user_login_model(stream_api:Namespace):
    auth = stream_api.model('Authentication', {
        'username': rest_fields.String(required=True),
        'password': rest_fields.String(required=True)
    })
    return auth

def user_register_model(stream_api:Namespace):
    user_metadata = stream_api.model('user_metadata', {
        'key': rest_fields.String(required=False),
        'value': rest_fields.String(required=False)
    })
    user_settings = stream_api.model('user_setting', {
        'key': rest_fields.String(required=False),
        'value': rest_fields.String(required=False)
    })
    reg_model = stream_api.model('Registration', {
        'username': rest_fields.String(required=True),
        'password': rest_fields.String(required=True),
        'user_role': rest_fields.String(required=True),
        'user_metadata': rest_fields.Nested(user_metadata, required=True),
        'user_settings': rest_fields.Nested(user_settings, required=True)
    })
    return reg_model

def zipstream_data_model(stream_api:Namespace):

    return stream_register_model(stream_api)


################################################################
#                     Response Models                          #
################################################################

def error_model(api:Namespace):
    resp = api.model('error_model', {
        'message': rest_fields.String
    })
    return resp


def auth_token_resp_model(api:Namespace):
    resp = api.model('auth_resp', {
        'access_token': rest_fields.String
    })
    return resp

def user_settings_resp_model(api:Namespace):
    resp = api.model('user_settings', {
        'user_settings': rest_fields.Arbitrary
    })
    return resp

def user_registration_resp_model(api:Namespace):
    resp = api.model('user_registration', {
        'message': rest_fields.Arbitrary
    })
    return resp

def stream_put_resp(api:Namespace):
    resp = api.model('stream_put_resp', {
        'message': rest_fields.String
    })
    return resp


def bucket_list_resp(api:Namespace):
    resp = api.model('bucket_list_resp', {
        'bucket-name': rest_fields.Raw({'last_modified': 'datetime'})
    })
    return resp


def object_list_resp(api:Namespace):
    desc = {"etag": "String",
            "content_type": "String",
            "is_dir": "Boolean",
            "object_name": "String",
            "metadata": "json",
            "size": "String",
            "bucket_name": "String",
            "last_modified": "Timestamp (Seconds)"}
    resp = api.model('object_list_resp', {
        'object-name': rest_fields.Raw(desc)
    })
    return resp


def object_stats_resp(api:Namespace):
    desc = {
        "size": "string",
        "object_name": "string",
        "bucket_name": "string",
        "etag": "string",
        "last_modified": "datetime",
        "content_type": "string",
        "is_dir": "string",
        "metadata": "{}"
    }
    resp = api.model('object_stats_resp', {
        'object-name': rest_fields.Raw(desc)
    })
    return resp
