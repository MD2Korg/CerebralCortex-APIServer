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

from flask_restplus import fields as rest_fields

def stream_data_model(stream_api):

    data_descriptor = stream_api.model('DataDescriptor', {
        'type1': rest_fields.String(required=True),
        'unit': rest_fields.String(required=True)
    })

    input_parameters = stream_api.model('InputParameters', {
        'window_size': rest_fields.Integer(required=True),
        'window_offset': rest_fields.Integer(required=True),
        'low_level_threshold': rest_fields.Float(required=True),
        'high_level_threshold': rest_fields.Float(required=True)
    })
    input_streams = stream_api.model('InputStreams', {
        'identifier': rest_fields.String(required=True),
        'name': rest_fields.String(required=True)
    })
    output_streams = stream_api.model('OutputStreams', {
        'identifier': rest_fields.String(required=True),
        'name': rest_fields.String(required=True)
    })
    algorithm_reference = stream_api.model('AlgorithmReference', {
        'url': rest_fields.String(required=True)
    })
    algorithm = stream_api.model('Algorithm', {
        'method': rest_fields.String(required=True),
        'description': rest_fields.String(required=True),
        'authors': rest_fields.Arbitrary(required=True),
        'version': rest_fields.String(required=True),
        'reference': rest_fields.List(rest_fields.Nested(algorithm_reference), required=True)
    })
    processing_module = stream_api.model('ProcessingModule', {
        'name': rest_fields.String(required=True),
        'description': rest_fields.String(required=True),
        'input_parameters': rest_fields.Nested(input_parameters, required=True),
        'input_streams': rest_fields.List(rest_fields.Nested(input_streams), required=True),
        'output_streams': rest_fields.List(rest_fields.Nested(output_streams), required=True),
        'algorithm': rest_fields.List(rest_fields.Nested(algorithm), required=True)
    })

    execution_context = stream_api.model('Execution Context', {
        'processing_module': rest_fields.Nested(processing_module, required=True)
    })

    annotations = stream_api.model('Annotation', {
        'name': rest_fields.String(required=True),
        'identifier': rest_fields.String(required=True)
        # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    })

    stream = stream_api.model('Stream', {
        'identifier': rest_fields.String(required=True),
        'owner': rest_fields.String(required=True),
        'name': rest_fields.String(required=True),
        'data_descriptor': rest_fields.List(rest_fields.Nested(data_descriptor), required=True),
        'execution_context': rest_fields.Nested(execution_context, required=True),
        'annotations': rest_fields.List(rest_fields.Nested(annotations),required=True)
    })
    return stream

def auth_data_model(stream_api):
    auth = stream_api.model('Authentication', {
        'email_id': rest_fields.String(required=True, type="email"),
        'password': rest_fields.String(required=True),
    })
    return auth


########################
#   Response Models
########################

def error_model(api):
    resp = api.model('error_model', {
        'message': rest_fields.String
    })
    return resp


def auth_token_resp_model(api):
    resp = api.model('auth_resp', {
        'access_token': rest_fields.String
    })
    return resp


def stream_put_resp(api):
    resp = api.model('stream_put_resp', {
        'message': rest_fields.String
    })
    return resp


def bucket_list_resp(api):
    resp = api.model('bucket_list_resp', {
        'bucket-name': rest_fields.Raw({'last_modified': 'datetime'})
    })
    return resp


def object_list_resp(api):
    desc = {"last_modified": "datetime", "size": "string",
            "content_type": "string", "etag": "string"}
    resp = api.model('object_list_resp', {
        'object-name': rest_fields.Raw(desc)
    })
    return resp


def object_stats_resp(api):
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
