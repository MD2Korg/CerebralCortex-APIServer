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

from datetime import datetime, timedelta

from flask import request
from flask_jwt_extended import create_access_token
from flask_restplus import Namespace, Resource

from .. import CC
from ..core.data_models import auth_data_model, error_model, auth_token_resp_model
from ..core.decorators import auth_required

auth_route = CC.configuration['routes']['auth']
auth_api = Namespace(auth_route, description='Authentication service')


@auth_api.route('/')
class Auth(Resource):
    @auth_api.doc('')
    @auth_api.expect(auth_data_model(auth_api), validate=True)
    @auth_api.response(400, 'User name and password cannot be empty.', model=error_model(auth_api))
    @auth_api.response(401, 'Invalid credentials.', model=error_model(auth_api))
    @auth_api.response(200, 'Authentication is approved', model=auth_token_resp_model(auth_api))
    def post(self):
        '''Post authentication credentials'''
        username = request.json.get('email_id', None).strip()
        password = request.json.get('password', None).strip()

        if not username or not password:
            return {"message": "User name and password cannot be empty."}, 401

        if not CC.login_user(username, password):
            return {"message": "Wrong username or password"}, 401

        token_issue_time = datetime.now()
        expires = timedelta(seconds=int(CC.configuration['apiserver']['token_expire_time']))
        token_expiry = token_issue_time + expires

        token = create_access_token(identity=username, expires_delta=expires)

        # Identity can be any data that is json serializable
        access_token = {"access_token": token}
        CC.update_auth_token(username, token, token_issue_time, token_expiry)
        return access_token, 200

    @auth_required
    @auth_api.header("Authorization", 'Bearer JWT', required=True)
    def get(self):
        '''Sample route to test authentication'''
        return "working!", 200
