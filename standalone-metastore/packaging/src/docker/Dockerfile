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
ONBUILD COPY hive-standalone-metastore-*-bin.tar.gz /opt

FROM ubuntu as archive
ARG HADOOP_VERSION
ARG HIVE_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
 wget https://downloads.apache.org/hive/hive-standalone-metastore-$HIVE_VERSION/hive-standalone-metastore-$HIVE_VERSION-bin.tar.gz
ONBUILD RUN mv hadoop-$HADOOP_VERSION.tar.gz /opt && \
 mv hive-standalone-metastore-$HIVE_VERSION-bin.tar.gz /opt

FROM ubuntu as buildarchive
ARG HADOOP_VERSION
ARG HIVE_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ONBUILD COPY ./hive-standalone-metastore-$HIVE_VERSION-bin.tar.gz /opt
ONBUILD RUN mv hadoop-$HADOOP_VERSION.tar.gz /opt

FROM ${BUILD_ENV} as env
RUN echo ${BUILD_ENV}
ARG HADOOP_VERSION
ARG HIVE_VERSION

RUN apt-get update && \
    apt-get install -y wget

RUN tar -xzv \
      --exclude="hadoop-$HADOOP_VERSION/include" \
      --exclude="hadoop-$HADOOP_VERSION/lib/native" \
      --exclude="hadoop-$HADOOP_VERSION/share/doc" \
      --exclude="hadoop-$HADOOP_VERSION/share/hadoop/client" \
      --exclude="hadoop-$HADOOP_VERSION/share/hadoop/tools" \
      --exclude="hadoop-$HADOOP_VERSION/share/hadoop/yarn/*" \
      --exclude="*/jdiff" \
      --exclude="*/sources" \
      --exclude="*tests.jar" \
      --exclude="*/webapps" \
      -f /opt/hadoop-$HADOOP_VERSION.tar.gz \
      -C /opt/ && \
    \
    find /opt/hadoop-$HADOOP_VERSION/share/hadoop/common/lib \
      \( -name "jetty-*.jar" -o -name "zookeeper-*.jar" -o -name "netty-*.jar" \) \
      -delete && \
    # Extract hadoop jars only
    tar -xzv  \
      -f /opt/hadoop-$HADOOP_VERSION.tar.gz \
      -C /opt/ \
      --wildcards "hadoop-$HADOOP_VERSION/share/hadoop/tools/lib/hadoop-*.jar" && \
    # INSTALL HIVE
    tar -xzvf /opt/hive-standalone-metastore-$HIVE_VERSION-bin.tar.gz -C /opt/

FROM eclipse-temurin:21.0.3_9-jre-ubi9-minimal AS run

ARG UID=1000
ARG HADOOP_VERSION
ARG HIVE_VERSION

# Install dependencies
RUN set -ex; \
    microdnf update -y; \
    microdnf -y install procps gettext; \
    microdnf clean all; \
    useradd --no-create-home -s /sbin/nologin -c "" --uid $UID hive

# Set necessary environment variables.
ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    HIVE_VER=$HIVE_VERSION

ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY --from=env --chown=hive /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME
COPY --from=env --chown=hive /opt/apache-hive-metastore-$HIVE_VERSION-bin $HIVE_HOME

COPY --chown=hive entrypoint.sh /
COPY --chown=hive conf $HIVE_HOME/conf

RUN chmod +x /entrypoint.sh && \
    mkdir -p $HIVE_HOME/data/warehouse && \
    chown hive $HIVE_HOME/data/warehouse

USER hive
WORKDIR $HIVE_HOME
EXPOSE 9001 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
