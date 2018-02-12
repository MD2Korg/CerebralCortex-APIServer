#Steps for executing locust to test Stream Api

1.Copy the Application load balancer url from outputs section of the parent CFT
2. Replace copied url in this line (host = "http://127.0.0.1:8088/api/v1") in the locust file present at {Path To Repo}/CerebralCortex-APIServer/test/locustfile.py this location
3.In the same file update the path for => data_dir .The path should be {Path To Repo}/CerebralCortex-APIServer/test/data/20180111/
4.You Have to install locust on your machine using below step If it is already present kindly ignore the the step
  - $ pip3 install locustio
5. To test the stream Api execute locust  file using below command
  $ locust -f ${path to the locust file}
6. locust will run on localhost:8089
7. open the locust UI on your browser and start new locust swarm


