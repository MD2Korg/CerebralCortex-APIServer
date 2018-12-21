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

from apiserver.apis.auth import auth_api as auth_v1
from apiserver.apis.object import object_api as object_v1
from apiserver.apis.stream_v2 import stream_api as stream_v2
# from apiserver.apis.stream_aws_s3 import stream_api_aws_s3 as stream_api_aws_s3_v1
from flask import Blueprint
from flask_restplus import Api

blueprint = Blueprint('v2', __name__, url_prefix="/api/v2")
api_doc = '/docs/'

api = Api(blueprint,
          title='Cerebral Cortex',
          version='3.0',
          description='API server for Cerebral Cortex',
          contact='dev@md2k.org',
          license='BSD 2-Clause',
          license_url='https://opensource.org/licenses/BSD-2-Clause',
          doc=api_doc
          )

api.add_namespace(auth_v1)
api.add_namespace(object_v1)
api.add_namespace(stream_v2)
# api.add_namespace(stream_api_aws_s3_v1)
