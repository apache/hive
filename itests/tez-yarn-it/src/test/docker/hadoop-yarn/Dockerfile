# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Custom Hadoop image for the Tez-on-YARN Integration tests.
#
# The official apache/hadoop:3.4.3 image ships Java 11 for its HDFS/YARN daemons.
# Hive is compiled for Java 21, so Tez AM and task containers -- which load
# hive-exec classes -- must run on Java 21 or later.  This image adds a Java 25
# JDK at /opt/jdk25 using a multi-stage build from eclipse-temurin:25-jdk; the
# daemons continue to use the default Java 11.
#
# Hadoop must be 3.4.3 or later: older UserGroupInformation calls
# Subject.getSubject, which always throws UnsupportedOperationException on
# JDK 24+ (JEP 486), so the Tez AM would fail to start on Java 25.
FROM --platform=linux/amd64 eclipse-temurin:25-jdk AS jdk25
FROM apache/hadoop:3.4.3

USER root
COPY --from=jdk25 /opt/java/openjdk /opt/jdk25
# Ensure the JDK binaries are executable and verify the copy is functional.
# Fails at image-build time if the JDK is incompatible with the base image's
# glibc, surfacing the problem early rather than silently at YARN container
# launch time (where YARN shows exit code 1 and empty stderr).
RUN chmod 755 /opt/jdk25/bin/java && /opt/jdk25/bin/java -version

USER hadoop
