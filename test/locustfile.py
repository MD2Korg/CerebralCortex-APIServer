from locust import HttpLocust, TaskSet, task
import requests
from os import listdir
from os.path import isfile, join

host = "http://127.0.0.1:5000/api/v1"
data_dir = "/home/ali/IdeaProjects/MD2K_DATA/raw14/"

class UserBehavior(TaskSet):

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """
        self.put_zipped_stream()

    @task(1)
    def put_zipped_stream(self):
        url = host+'/stream/zip'
        onlyfiles = [f for f in listdir(data_dir) if isfile(join(data_dir, f))]
        for payload_file in onlyfiles:
            payload = {'file': open(data_dir+payload_file, 'rb')}
            resp = requests.put(url, files=payload)
        #print(resp)

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    host = host
    min_wait = 5000
    max_wait = 9000