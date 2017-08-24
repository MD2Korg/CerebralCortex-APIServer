from locust import HttpLocust, TaskSet, task
import requests
import json
from os import listdir
from os.path import isfile, join

host = "http://127.0.0.1/api/v1"
#host = "http://127.0.0.1:8088/api/v1"
data_dir = "gz/raw14/"


class LoadTestApiServer(TaskSet):
    auth_token = ""

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        self.login_api_server()

    def login_api_server(self):
        # self.client.headers['Content-Type'] = "application/json"
        payload = {"email_id":"string", "password":"string"}
        response = self.client.post("/auth/", json=payload)
        print(response)
        json_response_dict = response.json()
        self.auth_token = json_response_dict["access_token"]

    @task(1)
    def get_auth(self):
        self.client.headers['Authorization'] = self.auth_token
        response = self.client.get("/auth/")
        print(response)


    @task(1)
    def put_zipped_stream(self):
        # self.client.headers['Content-Type'] = "multipart/form-data"
        self.client.headers['Authorization'] = self.auth_token
        onlyfiles = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]
        for payload_file in onlyfiles:
            self.client.put("/stream/zip/", files={'file': open(data_dir+payload_file, 'rb')})


class WebsiteUser(HttpLocust):
    task_set = LoadTestApiServer
    host = host
    min_wait = 100
    max_wait = 500
