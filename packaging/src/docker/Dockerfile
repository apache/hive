#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8-jre

ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install curl; \
  apt-get -y install procps; \
  rm -rf /var/lib/apt/lists/*

COPY hadoop-$HADOOP_VERSION.tar.gz /opt
COPY apache-hive-$HIVE_VERSION-bin.tar.gz /opt
COPY apache-tez-$TEZ_VERSION-bin.tar.gz /opt

RUN tar -xzvf /opt/hadoop-$HADOOP_VERSION.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \
    tar -xzvf /opt/apache-hive-$HIVE_VERSION-bin.tar.gz -C /opt/ && \
    ln -s /opt/apache-hive-$HIVE_VERSION-bin /opt/hive && \
    tar -xzvf /opt/apache-tez-$TEZ_VERSION-bin.tar.gz -C /opt && \
    ln -s /opt/apache-tez-$TEZ_VERSION-bin /opt/tez

# Set necessary environment variables.
ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    TEZ_HOME=/opt/tez

ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY scripts/docker-entrypoint.sh /
COPY conf $HIVE_HOME/conf
RUN chmod +x /docker-entrypoint.sh

ARG UID=1000
RUN adduser --no-create-home --disabled-login --gecos "" --uid $UID hive && \
    chown hive /opt/tez && \
    chown hive /opt/hive && \
    chown hive /opt/hive/conf && \
    chown hive /opt/hadoop && \
    mkdir -p /opt/hive/data/warehouse && \
    chown hive /opt/hive/data/warehouse


USER hive
WORKDIR /opt/hive
EXPOSE 10000 10002 9083
ENTRYPOINT ["sh", "-c", "/docker-entrypoint.sh"]