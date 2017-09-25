import json
from os import listdir
from os.path import isfile, join

from locust import HttpLocust, TaskSet, task

# ali config
host = "http://127.0.0.1:8088/api/v1"
data_dir = "/home/ali/IdeaProjects/MD2K_DATA/raw14/"

# tim config
# host = "http://127.0.0.1/api/v1"
# host = "https://127.0.0.1/api/v1"
# host = "https://md2k-hnat/api/v1"
# data_dir = "gz/raw14/"
#host = "https://fourtytwo.md2k.org/api/v1"
#data_dir = "gz/raw14/"

default_metadata = {
    "identifier": "0bf18489-bc04-42d9-8ded-dc54e686a67a",
    "name": "string",
    "data_descriptor": [
        {
            "unit": "string",
            "type": "string"
        }
    ],
    "owner": "7547cb22-c1a9-42ca-ac73-a7ddcc8a0a30",
    "execution_context": {}
}


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
        onlyfiles = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]
        for payload_file in onlyfiles:
            uploaded_file = dict(file=open(data_dir + payload_file, 'rb'))

            self.client.put("/stream/zip/", files=uploaded_file, data={'metadata': json.dumps(default_metadata)})
            uploaded_file['file'].close()


class WebsiteUser(HttpLocust):
    task_set = LoadTestApiServer
    host = host
    min_wait = 1000
    max_wait = 1500
