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

from datetime import timedelta

from flask import Flask
from flask_jwt_extended import JWTManager

from apiserver import apiserver_config
from apiv3 import blueprint as api3

app = Flask(__name__)

app.config['JWT_SECRET_KEY'] = apiserver_config['apiserver']['secret_key']
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(seconds=int(apiserver_config['apiserver']['token_expire_time']))

JWTManager(app)
app.secret_key = apiserver_config['apiserver']['secret_key']

#app.register_blueprint(api1)
app.register_blueprint(api3 )

if __name__ == "__main__":
        '''
        command line args
        -c Configuration dir path
        -od Directory path where all the gz files will be stored by API-Server
        '''

        app.run(debug=apiserver_config['apiserver']['debug'], host=apiserver_config['apiserver']['host'],
                port=apiserver_config['apiserver']['port'])
