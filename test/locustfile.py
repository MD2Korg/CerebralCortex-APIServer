import json
from os import listdir
from os.path import isfile, join

from locust import HttpLocust, TaskSet, task

# ali config
host = "http://127.0.0.1:8088/api/v1"
data_dir = "/home/ali/IdeaProjects/MD2K_DATA/CC_apiserver_files/"

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
        response = self.client.post("/auth/", json=payload)
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
