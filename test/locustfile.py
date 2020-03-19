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
from os import listdir
from os.path import join

from locust import HttpLocust, TaskSet, task

# ali config
host = "http://127.0.0.1:8089/api/v3"
data_dir = "/home/ali/IdeaProjects/CerebralCortex-DockerCompose/data/20171211/"

# tim config
# host = "http://127.0.0.1/api/v1"
# host = "https://127.0.0.1/api/v1"
# host = "https://md2k-hnat/api/v1"
# data_dir = "gz/raw14/"
#host = "https://fourtytwo.md2k.org/api/v1"
#data_dir = "gz/raw14/"


class LoadTestApiServer(TaskSet):
    auth_token = ""

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        # self.login_api_server()
        self.client.verify = False
        pass

    @task(1)
    def api_flow(self):
        self.login_api_server()
        self.put_zipped_stream()


    def login_api_server(self):
        payload = {"username": "string", "password": "string"}
        response = self.client.post("/default/login/", json=payload)
        json_response_dict = response.json()
        self.auth_token = json_response_dict["access_token"]

    # @task(1)
    def put_zipped_stream(self):
        self.client.headers['Authorization'] = self.auth_token
        onlyfiles = [f for f in listdir(data_dir) if (join(data_dir, f)).endswith('.json')]
        for payload_file in onlyfiles:
            uploaded_file = dict(file=open(data_dir + payload_file.replace('.json','.gz'), 'rb'))
            metadata = open(data_dir + payload_file, 'r')
            self.client.put("/stream/zip/", files=uploaded_file, data={'metadata': json.dumps(metadata.read())})
            uploaded_file['file'].close()
            metadata.close()



class WebsiteUser(HttpLocust):
    task_set = LoadTestApiServer
    host = host
    min_wait = 1000
    max_wait = 1500
