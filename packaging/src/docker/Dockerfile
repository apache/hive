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
ARG BUILD_ENV=local

FROM ubuntu AS local
ONBUILD COPY hadoop-*.tar.gz /opt
ONBUILD COPY apache-hive-*-bin.tar.gz /opt
ONBUILD COPY apache-tez-*-bin.tar.gz /opt

FROM ubuntu AS release
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget --no-verbose https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz && \
 wget --no-verbose https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
 wget --no-verbose https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
ONBUILD RUN mv /apache-tez-$TEZ_VERSION-bin.tar.gz /opt && \
 mv hadoop-$HADOOP_VERSION.tar.gz /opt && \
 mv apache-hive-$HIVE_VERSION-bin.tar.gz /opt

FROM ubuntu AS hybrid
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
ONBUILD RUN apt-get update && apt-get -y install wget
ONBUILD RUN wget --no-verbose https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz && \
 wget --no-verbose https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ONBUILD COPY ./apache-hive-$HIVE_VERSION-bin.tar.gz /opt
ONBUILD RUN mv /apache-tez-$TEZ_VERSION-bin.tar.gz /opt && \
 mv hadoop-$HADOOP_VERSION.tar.gz /opt

FROM ${BUILD_ENV} AS env
RUN echo ${BUILD_ENV}
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION

RUN tar -xzv \
      --exclude="hadoop-$HADOOP_VERSION/share/doc" \
      --exclude="*/jdiff" \
      --exclude="*/sources" \
      --exclude="*tests.jar" \
      --exclude="*/webapps" \
      -f /opt/hadoop-$HADOOP_VERSION.tar.gz \
      -C /opt/ && \
    # INSTALL HIVE
    tar -xzv \
      --exclude="apache-hive-$HIVE_VERSION-bin/jdbc" \
      -f /opt/apache-hive-$HIVE_VERSION-bin.tar.gz \
      -C /opt/ && \
    # INSTALL TEZ
    tar -xzv \
      --exclude="apache-tez-$TEZ_VERSION-bin/share" \
      -f /opt/apache-tez-$TEZ_VERSION-bin.tar.gz \
      -C /opt; \
    mkdir -p /opt/tez-snapshot;

FROM eclipse-temurin:21-jdk-ubi9-minimal AS run

ARG UID=1000
ARG HADOOP_VERSION
ARG HIVE_VERSION
ARG TEZ_VERSION
ARG TEZ_SNAPSHOT_VERSION=
ARG TEZ_SNAPSHOT_REPO_URL=https://repository.apache.org/content/repositories/snapshots

# Install dependencies
RUN set -ex; \
    microdnf update -y; \
    microdnf -y install procps gettext wget xmlstarlet; \
    microdnf clean all; \
    useradd --no-create-home -s /sbin/nologin -c "" --uid $UID hive

# Set necessary environment variables.
ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    TEZ_HOME=/opt/tez \
    HIVE_VER=$HIVE_VERSION

ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY --from=env --chown=hive /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME
COPY --from=env --chown=hive /opt/apache-hive-$HIVE_VERSION-bin $HIVE_HOME
COPY --from=env --chown=hive /opt/apache-tez-$TEZ_VERSION-bin $TEZ_HOME
COPY --from=env --chown=hive /opt/tez-snapshot /opt/tez-snapshot

# When TEZ_SNAPSHOT_VERSION is set, fetch Tez snapshot jars from the Maven snapshot repository
# and place them under /opt/tez-snapshot. At runtime, entrypoint.sh symlinks these into
# $HIVE_HOME/lib with a "0-" prefix so they sort first in bin/hive's classpath glob, ensuring
# snapshot classes take precedence over the Tez release jars bundled with Hive.
# Maven snapshot repositories use timestamped filenames (e.g. tez-api-1.0.0-20250101.jar),
# so we fetch maven-metadata.xml first to resolve the exact filename before downloading the jar.
RUN set -eux; \
    mkdir -p /opt/tez-snapshot-download; \
    if [[ -n "${TEZ_SNAPSHOT_VERSION}" ]]; then \
      base_url="${TEZ_SNAPSHOT_REPO_URL}/org/apache/tez"; \
      for artifact in tez-common tez-api tez-dag tez-mapreduce tez-runtime-internals tez-runtime-library; do \
        version_url="${base_url}/${artifact}/${TEZ_SNAPSHOT_VERSION}"; \
        metadata_url="${version_url}/maven-metadata.xml"; \
        metadata_file="/opt/tez-snapshot-download/${artifact}-maven-metadata.xml"; \
        echo "metadata_url=${metadata_url}"; \
        wget -q "${metadata_url}" -O "${metadata_file}"; \
        snapshot_value="$(xmlstarlet sel -t -v "string(/metadata/versioning/snapshotVersions/snapshotVersion[extension='jar' and not(classifier)]/value)" "${metadata_file}")"; \
        test -n "${snapshot_value}"; \
        jar_file="${artifact}-${snapshot_value}.jar"; \
        jar_url="${version_url}/${jar_file}"; \
        echo "jar_url=${jar_url}"; \
        wget -q "${jar_url}" -O "/opt/tez-snapshot/${jar_file}"; \
      done; \
      echo "Downloaded Tez snapshot jars under /opt/tez-snapshot:"; \
      ls -1 /opt/tez-snapshot/*.jar; \
    else \
      echo "TEZ_SNAPSHOT_VERSION not set. Skipping Tez snapshot download."; \
    fi; \
    rm -rf /opt/tez-snapshot-download


COPY --chown=hive entrypoint.sh /
COPY --chown=hive conf $HIVE_HOME/conf

RUN chmod +x /entrypoint.sh && \
    mkdir -p $HIVE_HOME/data/warehouse && \
    chown hive $HIVE_HOME/data/warehouse && \
    mkdir -p $HIVE_HOME/scratch && \
    chown hive $HIVE_HOME/scratch && \
    mkdir -p /home/hive/.beeline && \
    chown hive /home/hive/.beeline && \
    chown -R hive /opt/tez-snapshot

USER hive
WORKDIR $HIVE_HOME
EXPOSE 10000 10002 9083

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]
