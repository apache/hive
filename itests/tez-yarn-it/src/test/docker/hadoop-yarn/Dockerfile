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

# Custom Hadoop image for the Tez-on-YARN localization integration tests.
#
# The official apache/hadoop:3.4.2 image ships Java 8 for its HDFS/YARN daemons.
# Hive is compiled for Java 21, so Tez AM and task containers -- which load
# hive-exec classes -- must run on Java 21.  This image adds a Java 21 JDK at
# /opt/jdk21 using a multi-stage build from eclipse-temurin:21-jdk; the daemons
# continue to use the default Java 8.
FROM eclipse-temurin:21-jdk AS jdk21
FROM apache/hadoop:3.4.2

USER root
COPY --from=jdk21 /opt/java/openjdk /opt/jdk21
USER hadoop
