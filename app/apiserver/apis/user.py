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


import json

from flask import request
from flask_restplus import Namespace, Resource

from .. import CC, apiserver_config
from ..core.data_models import user_login_model, user_register_model, error_model, auth_token_resp_model, \
    user_settings_resp_model, user_registration_resp_model
from ..core.decorators import auth_required

auth_route = apiserver_config['routes']['user']
auth_api = Namespace(auth_route, description='Authentication service')


@auth_api.route('')
class Auth(Resource):
    def get(self):
        return {"message": "user route is working"}, 200


@auth_api.route('/register')
class Auth(Resource):
    @auth_api.doc('')
    @auth_api.expect(user_register_model(auth_api), validate=True)
    @auth_api.response(400, 'All fields are required.', model=error_model(auth_api))
    @auth_api.response(401, 'Invalid credentials.', model=error_model(auth_api))
    @auth_api.response(200, 'User registration successful.', model=user_registration_resp_model(auth_api))
    def post(self):
        '''Post required fields (username, password, user_role, user_metadata, user_settings) to register a user'''
        try:
            username = request.get_json().get('username', None).strip()
            user_password = request.get_json().get('password', None).strip()
            user_role = request.get_json().get('user_role', None).strip()
            user_metadata = request.get_json().get('user_metadata', None)
            user_settings = request.get_json().get('user_settings', None)
            status = CC.create_user(username, user_password, user_role, user_metadata, user_settings)
            if status:
                return {"message": str(username) + " is created successfully."}, 200
            else:
                return {"message": "Cannot create, something went wrong."}, 400
        except (ValueError, Exception) as err:
            return {"message": str(err)}, 400


@auth_api.route('/login')
class Auth(Resource):
    @auth_api.doc('')
    @auth_api.expect(user_login_model(auth_api), validate=True)
    @auth_api.response(400, 'User name and password cannot be empty.', model=error_model(auth_api))
    @auth_api.response(401, 'Invalid credentials.', model=error_model(auth_api))
    @auth_api.response(200, 'Authentication is approved', model=auth_token_resp_model(auth_api))
    def post(self):
        """
        authenticate a user
        """
        username = request.get_json().get('username', None)
        password = request.get_json().get('password', None)
        if not username or not password:
            return {"message": "User name and password cannot be empty."}, 401

        login_status = CC.connect(username, password, encrypted_password=False)

        if login_status.get("status", False) == False:
            return {"message": login_status.get("msg", "no-message-available")}, 401

        token = login_status.get("auth_token")
        user_uuid = CC.get_user_id(username)
        
        access_token = {"auth_token": token, 'user_uuid': user_uuid}
        return access_token, 200


@auth_api.route('/config')
class Auth(Resource):
    @auth_api.doc('')
    @auth_required
    @auth_api.header("Authorization", 'Bearer <JWT>', required=True)
    @auth_api.response(400, 'Authorization code cannot be empty.', model=error_model(auth_api))
    @auth_api.response(401, 'Invalid credentials.', model=error_model(auth_api))
    @auth_api.response(200, 'Request successful', model=user_settings_resp_model(auth_api))
    def get(self):
        '''Post required fields (username, password, user_role, user_metadata, user_settings) to register a user'''
        token = request.headers['Authorization']
        token = token.replace("Bearer ", "")

        try:
            user_settings = CC.get_user_settings(auth_token=token)
            return {"user_settings": json.dumps(user_settings)}
        except Exception as e:
            return {"message", str(e)}, 400
