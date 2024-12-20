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
ARG BUILD_ENV

FROM ubuntu as unarchive
ONBUILD COPY hadoop-*.tar.gz /opt
ONBUILD COPY apache-hive-*-bin.tar.gz /opt
ONBUILD COPY apache-tez-*-bin.tar.gz /opt

FROM ubuntu as archive
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz && \
 wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
 wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
ONBUILD RUN mv /apache-tez-$TEZ_VERSION-bin.tar.gz /opt && \
 mv hadoop-$HADOOP_VERSION.tar.gz /opt && \
 mv apache-hive-$HIVE_VERSION-bin.tar.gz /opt

FROM ubuntu as buildarchive
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz && \
 wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ONBUILD COPY ./apache-hive-$HIVE_VERSION-bin.tar.gz /opt
ONBUILD RUN mv /apache-tez-$TEZ_VERSION-bin.tar.gz /opt && \
 mv hadoop-$HADOOP_VERSION.tar.gz /opt

FROM ${BUILD_ENV} as env
RUN echo ${BUILD_ENV}
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION

RUN tar -xzvf /opt/hadoop-$HADOOP_VERSION.tar.gz -C /opt/ && \
    rm -rf /opt/hadoop-$HADOOP_VERSION/share/doc/* && \
    tar -xzvf /opt/apache-hive-$HIVE_VERSION-bin.tar.gz -C /opt/ && \
    rm -rf /opt/apache-hive-$HIVE_VERSION-bin/jdbc/* && \
    tar -xzvf /opt/apache-tez-$TEZ_VERSION-bin.tar.gz -C /opt && \
    rm -rf /opt/apache-tez-$TEZ_VERSION-bin/share/*

FROM openjdk:8-jre-slim AS run

ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
COPY --from=env /opt/hadoop-$HADOOP_VERSION /opt/hadoop
COPY --from=env /opt/apache-hive-$HIVE_VERSION-bin /opt/hive
COPY --from=env /opt/apache-tez-$TEZ_VERSION-bin /opt/tez

# Install dependencies
RUN set -ex; \
    apt-get update; \
    apt-get -y install procps; \
    rm -rf /var/lib/apt/lists/*

# Set necessary environment variables.
ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    TEZ_HOME=/opt/tez \
    HIVE_VER=$HIVE_VERSION

ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY entrypoint.sh /
COPY conf $HIVE_HOME/conf
RUN chmod +x /entrypoint.sh


ARG UID=1000
RUN adduser --no-create-home --disabled-login --gecos "" --uid $UID hive && \
    chown hive /opt/tez && \
    chown hive /opt/hive && \
    chown hive /opt/hadoop && \
    chown hive /opt/hive/conf && \
    mkdir -p /opt/hive/data/warehouse && \
    chown hive /opt/hive/data/warehouse && \
    mkdir -p /home/hive/.beeline && \
    chown hive /home/hive/.beeline

USER hive
WORKDIR /opt/hive
EXPOSE 10000 10002 9083
ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
