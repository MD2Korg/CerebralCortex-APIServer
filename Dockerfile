FROM tiangolo/uwsgi-nginx-flask:python3.6
MAINTAINER Timothy Hnat twhnat@memphis.edu

#Install here to speed up docker image recreation for new versions of CerebralCortex
RUN pip install numpy scipy sklearn matplotlib minio kafka marshmallow flask
RUN pip install py4j pytz==2017.2 mysql-connector-python-rf==2.2.2 PyYAML==3.12 fastdtw addict

# Install Cerebral Cortex libraries for use in the notebook environment
RUN git clone https://github.com/MD2Korg/CerebralCortex \
    && cd CerebralCortex && python3 setup.py install \
    && cd .. && rm -rf CerebralCortex

# Python3 installs
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./app /app


COPY nginx/nginx.conf /etc/nginx/

RUN mkdir -p /data

VOLUME /data
