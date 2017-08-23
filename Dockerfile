FROM tiangolo/uwsgi-nginx-flask:python3.6
MAINTAINER Timothy Hnat twhnat@memphis.edu

#Install here to speed up docker image recreation for new versions of CerebralCortex
RUN pip install numpy scipy sklearn matplotlib minio kafka

# Python3 installs
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./app /app



# Install Cerebral Cortex libraries for use in the notebook environment
RUN git clone https://github.com/MD2Korg/CerebralCortex \
    && cd CerebralCortex && python3 setup.py install \
    && cd .. && rm -rf CerebralCortex
