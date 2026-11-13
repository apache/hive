# syntax=docker/dockerfile:1
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

# The image the Hive precommit (see Jenkinsfile) builds and tests Hive in.
#
# It is deliberately minimal: a JDK, Maven, and the handful of tools the Jenkinsfile
# stages and the tests shell out to. Everything else is fetched by Maven at build time.
# See README.md next to this file before adding anything.

# glibc based on purpose: tests run prebuilt native code linked against glibc
# (e.g. the Kudu mini cluster binaries, Conscrypt).
ARG DEBIAN_VERSION=trixie
ARG JAVA_VERSION=21

# The Temurin JDK is a generic glibc build; only the distribution of its image differs.
FROM eclipse-temurin:${JAVA_VERSION}-jdk AS jdk
# jmods are only used by jlink; classes_nocoops.jsa is only used with heaps > 32GB.
RUN rm -rf /opt/java/openjdk/jmods \
           /opt/java/openjdk/man \
           /opt/java/openjdk/lib/server/classes_nocoops.jsa

FROM debian:${DEBIAN_VERSION}-slim AS downloads
ARG MAVEN_VERSION=3.9.16
ARG APACHE_ARCHIVE=https://archive.apache.org/dist
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp

# Fetches $1 and verifies it against the published .sha512 (plain or BSD-style).
COPY <<'EOF' /usr/local/bin/fetch
#!/bin/sh
set -eu
curl -fsSL --retry 5 -o "${1##*/}" "$1"
echo "$(curl -fsSL --retry 5 "$1.sha512" | awk '{print $NF}')  ${1##*/}" | sha512sum -c -
EOF
RUN chmod +x /usr/local/bin/fetch

RUN fetch "${APACHE_ARCHIVE}/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    && mkdir /opt/maven \
    && tar -xzf apache-maven-${MAVEN_VERSION}-bin.tar.gz -C /opt/maven --strip-components=1 \
    && rm apache-maven-${MAVEN_VERSION}-bin.tar.gz

FROM debian:${DEBIAN_VERSION}-slim

# The image is published as apache/hive-ci-runner:<version>, and a published version is never
# overwritten: bump the version with every change of the image (see README.md).
LABEL org.opencontainers.image.version="1"
LABEL org.opencontainers.image.title="Apache Hive precommit" \
      org.opencontainers.image.description="Build and test environment of the Apache Hive precommit job" \
      org.opencontainers.image.source="https://github.com/apache/hive" \
      org.opencontainers.image.licenses="Apache-2.0"

# The base image already has the GNU bash, coreutils, diffutils, findutils, grep, sed and tar
# that scripts and tests expect (e.g. qtest result diffing, Hadoop's Shell/Stat/DF/DU helpers).
# ca-certificates, git: Jenkins checkout.
# docker-cli: tests start databases in the dind sidecar.
# libcom-err2, libk5crypto3, libkeyutils1, libkrb5support0: needed by the Kudu mini cluster
#   binaries (kudu-binary) that the kudu-handler tests start.
# openssl: tests generate self-signed certificates with it (e.g. GcsOAuthTokenMock).
# procps: ps, used by tests; python3, python-is-python3: TRANSFORM scripts in qtests;
# rsync: workspace hand-over between stages; tini: reaps the processes tests leave behind;
# xmlstarlet: pom.xml format check.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        docker-cli \
        git \
        libcom-err2 \
        libk5crypto3 \
        libkeyutils1 \
        libkrb5support0 \
        openssl \
        procps \
        python-is-python3 \
        python3 \
        rsync \
        tini \
        xmlstarlet \
    && rm -rf /var/lib/apt/lists/* \
    && git config --system --add safe.directory '*'

COPY --from=jdk /opt/java/openjdk /opt/java/openjdk
COPY --from=downloads /opt/maven /opt/maven

ENV JAVA_HOME=/opt/java/openjdk \
    MAVEN_HOME=/opt/maven \
    PATH=/opt/java/openjdk/bin:/opt/maven/bin:$PATH \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC

# The Jenkins agent runs as uid 1000 and the pod uses fsGroup 1000; tests must not run as root.
# ~/.m2 exists up front so that a local repository mounted below it leaves it writable.
ARG UID=1000
RUN groupadd -g ${UID} jenkins \
    && useradd -m -u ${UID} -g jenkins -d /home/jenkins -s /bin/bash jenkins \
    && install -d -o jenkins -g jenkins /home/jenkins/.m2

# docker run sets no USER, but LLAP reads it to submit work (LlapTaskCommunicator).
ENV USER=jenkins
USER jenkins
WORKDIR /home/jenkins

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
