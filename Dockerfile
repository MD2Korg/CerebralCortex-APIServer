FROM tiangolo/uwsgi-nginx-flask:python3.6

LABEL maintainer="Timothy Hnat <twhnat@memphis.edu>"
LABEL org.md2k.apiserver.version='3.1.0'
LABEL description="Cerebral Cortex REST API Server"


# Spark dependencies
ENV APACHE_SPARK_VERSION 2.4.4
ENV HADOOP_VERSION 2.7

ENV SPARK_HOME  /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip
ENV JAVA_HOME   /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH        $JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON python3
ENV HADOOP_HOME	   /opt/hadoop


HEALTHCHECK --interval=1m --timeout=3s --start-period=30s \
CMD curl -f http://localhost/api/v3/docs/ || exit 1

RUN apt-get update \
  && apt-get install -yqq libsnappy-dev wget git python3-pip  openjdk-8-jre python3-setuptools libyaml-dev libev-dev liblapack-dev \
  && pip3 install --upgrade --force-reinstall pip==9.0.3 \
  && pip3 install cython py4j

RUN \
  wget http://apache.mirrors.tds.net/hadoop/common/hadoop-3.1.3/hadoop-3.1.3.tar.gz && \
  tar -xzf hadoop-3.1.3.tar.gz && \
  mv hadoop-3.1.3 $HADOOP_HOME && \
  echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
  echo "PATH=$PATH:$HADOOP_HOME/bin" >> ~/.bashrc
 


RUN cd /tmp && \
        wget -q http://apache.cs.utah.edu/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
        tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local && \
        rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark




# Python3 installs
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt
COPY ./app /app

COPY nginx/nginx.conf /etc/nginx/

RUN mkdir -p /data /cc_config_file /cc_bucket

VOLUME /data /cc_bucket