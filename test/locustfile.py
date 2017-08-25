from locust import HttpLocust, TaskSet, task
import requests
import json
from os import listdir
from os.path import isfile, join

#host = "http://127.0.0.1/api/v1"
# host = "http://md2k-hnat/api/v1"
host = "http://127.0.0.1:8088/api/v1"
#data_dir = "gz/raw14/"
data_dir = "/home/ali/IdeaProjects/MD2K_DATA/raw14/"


class LoadTestApiServer(TaskSet):
    auth_token = ""

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        pass

    @task(1)
    def api_flow(self):
        self.login_api_server()
        self.put_zipped_stream

    def login_api_server(self):
        payload = {"email_id":"string", "password":"string"}
        response = self.client.post("/auth/", json=payload)
        json_response_dict = response.json()
        self.auth_token = json_response_dict["access_token"]


    def put_zipped_stream(self):
        self.client.headers['Authorization'] = self.auth_token
        onlyfiles = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]
        for payload_file in onlyfiles:
            payload = dict(file=open(data_dir+payload_file, 'rb'))
            self.client.put("/stream/zip/", files=payload)


class WebsiteUser(HttpLocust):
    task_set = LoadTestApiServer
    host = host
    min_wait = 500
    max_wait = 1500
