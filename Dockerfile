FROM tiangolo/uwsgi-nginx-flask:python3.6

LABEL maintainer="Timothy Hnat <twhnat@memphis.edu>"
LABEL org.md2k.apiserver.version='2.3.0'
LABEL description="Cerebral Cortex REST API Server"

HEALTHCHECK --interval=1m --timeout=3s --start-period=30s \
CMD curl -f http://localhost/api/v1/docs/ || exit 1

# Install Cerebral Cortex libraries for use in the notebook environment
RUN git clone https://github.com/MD2Korg/CerebralCortex -b 2.3.0 \
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

RUN mkdir -p /data /cc_config_file /cc_data

VOLUME /data /cc_data
