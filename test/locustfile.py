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

import os

from locust import HttpLocust, TaskSet, task

host = "http://127.0.0.1/api/v3"


class LoadTestApiServer(TaskSet):
    auth_token = ""

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        # self.login_api_server()
        self.client.verify = False
        #self.register_user()
        #self.login_api_server()
        #self.register_stream_api_server()

    @task(1)
    def api_flow(self):

        self.put_zipped_stream()

    def register_user(self):
        payload = {
          "username": "string",
          "password": "string",
          "user_role": "string",
          "user_metadata": {
            "key": "string",
            "value": "string"
          },
          "user_settings": {
            "key": "string",
            "value": "string"
          }
        }
        self.client.post("/user/default/register", json=payload)


    def login_api_server(self):
        payload = {"username": "string", "password": "string"}
        response = self.client.post("/user/default/login", json=payload)
        json_response_dict = response.json()
        self.auth_token = json_response_dict["auth_token"]

    def register_stream_api_server(self):
        self.client.headers['Authorization'] = self.auth_token
        metadata = {
                                            "name": "stress-test-stream-name-temporary-remove-it",
                                            "description": "some description",
                                            "data_descriptor": [
                                                {
                                                    "name": "timestamp",
                                                    "type": "datetime",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                },{
                                                    "name": "localtime",
                                                    "type": "datetime",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                },{
                                                    "name": "battery",
                                                    "type": "string",
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                }
                                            ],
                                            "modules": [
                                                {
                                                    "name": "string",
                                                    "version": "1.0",
                                                    "authors": [
                                                        {
                                                            "name": "Nasir Ali",
                                                            "email": "nasir.ali08@gmail.com",
                                                            "attributes": {
                                                                "key": "string",
                                                                "value": "string"
                                                            }
                                                        }
                                                    ],
                                                    "attributes": {
                                                        "key": "string",
                                                        "value": "string"
                                                    }
                                                }
                                            ]
                                        }
        self.client.post("/stream/default/register", json=metadata)

    # @task(1)
    def put_zipped_stream(self):
        self.client.headers['Authorization'] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6InN0cmluZyIsInRva2VuX2V4cGlyZV9hdCI6IjIwMjAtMDMtMjkgMDI6NTk6MzUuNjYyNDY5IiwidG9rZW5faXNzdWVkX2F0IjoiMjAyMC0wMy0yOCAxNTo1Mjo1NS42NjI0NjkifQ.tzdsb_TYDSVI1qP7CqYwEN6nerftgFRHanDL5ShQ7Vw"
        data_file = os.getcwd() + "/sample_data/msgpack/phone_battery_stream.gz"
        payload_file = dict(file=open(data_file, 'rb'))
        self.client.put("/stream/default/7a253634-61d2-382d-b9a9-70f9331df52e", files=payload_file)


class WebsiteUser(HttpLocust):
    task_set = LoadTestApiServer
    host = host
    min_wait = 1000
    max_wait = 1500
