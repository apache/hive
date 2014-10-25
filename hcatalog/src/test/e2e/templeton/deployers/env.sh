#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#set -x;

# define necessary env vars here and source it in other files

export HADOOP_VERSION=2.4.1-SNAPSHOT
#export HIVE_VERSION=0.14.0-SNAPSHOT
export PIG_VERSION=0.12.2-SNAPSHOT

#Root of project source tree
export PROJ_HOME=/Users/${USER}/dev/hive
export HIVE_HOME=${PROJ_HOME}/packaging/target/apache-hive-${HIVE_VERSION}-bin/apache-hive-${HIVE_VERSION}-bin
export HADOOP_HOME=/Users/${USER}/dev/hwxhadoop/hadoop-dist/target/hadoop-${HADOOP_VERSION}

#Make sure Pig is built for the Hadoop version you are running
export PIG_TAR_PATH=/Users/${USER}/dev/pig-${PIG_VERSION}-src/build
#this is part of Pig distribution
export PIG_PIGGYBANK_PATH=/Users/${USER}/dev/pig-${PIG_VERSION}-src/build/tar/pig-${PIG_VERSION}/contrib/piggybank/java/piggybank.jar

export WEBHCAT_LOG_DIR=/tmp/webhcat_e2e/logs
export WEBHCAT_PID_DIR=${WEBHCAT_LOG_DIR}
#config/hive/hive-site.xml should match this path - it doesn't understand env vars
export METASTORE_DB=${WEBHCAT_LOG_DIR}/wehcat_e2e_metastore_db
