FROM tiangolo/uwsgi-nginx-flask:python3.6
MAINTAINER Timothy Hnat twhnat@memphis.edu

# Install Cerebral Cortex libraries for use in the notebook environment
RUN git clone https://github.com/MD2Korg/CerebralCortex -b 2.2.1 \
    && cd CerebralCortex \
    && pip3 install -r requirements.txt \
    && python3 setup.py install \
    && cd .. && rm -rf CerebralCortex

# Python3 installs
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./app /app


COPY nginx/nginx.conf /etc/nginx/

RUN mkdir -p /data /cc_config_file

VOLUME /data
