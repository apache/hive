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

#todo: most of these variables are defined in pom.xml - see this can be integrated
echo ${HADOOP_VERSION};

if [ -z ${HADOOP_VERSION} ]; then
  export HADOOP_VERSION=2.4.1-SNAPSHOT
fi

if [ -z ${HIVE_VERSION} ]; then
  export HIVE_VERSION=0.14.0-SNAPSHOT
fi

if [ -z ${PIG_VERSION} ]; then
  export PIG_VERSION=0.12.2-SNAPSHOT
fi

if [ -z ${TEZ_VERSION} ]; then
  export TEZ_VERSION=0.5.3
fi

#Root of project source tree
current_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../../../.. && pwd )
export PROJ_HOME=`dirname $current_dir`
export HIVE_HOME=${PROJ_HOME}/packaging/target/apache-hive-${HIVE_VERSION}-bin/apache-hive-${HIVE_VERSION}-bin

if [ -z ${HADOOP_HOME} ]; then
  export HADOOP_HOME=/Users/${USER}/dev/hwxhadoop/hadoop-dist/target/hadoop-${HADOOP_VERSION}
fi

if [ -z ${MYSQL_CLIENT_JAR} ]; then
  #if using MySQL backed metastore
  export MYSQL_CLIENT_JAR=/Users/${USER}/dev/mysql-connector-java-5.1.30/mysql-connector-java-5.1.30-bin.jar
fi

export TEZ_CLIENT_HOME=/Users/ekoifman/dev/apache-tez-client-${TEZ_VERSION}
#Make sure Pig is built for the Hadoop version you are running
export PIG_TAR_PATH=/Users/${USER}/dev/pig-${PIG_VERSION}-src/build
#this is part of Pig distribution
export PIG_PIGGYBANK_PATH=/Users/${USER}/dev/pig-${PIG_VERSION}-src/build/tar/pig-${PIG_VERSION}/contrib/piggybank/java/piggybank.jar

export WEBHCAT_LOG_DIR=/tmp/webhcat_e2e/logs
export WEBHCAT_PID_DIR=${WEBHCAT_LOG_DIR}
#config/hive/hive-site.xml should match this path - it doesn't understand env vars
export METASTORE_DB=${WEBHCAT_LOG_DIR}/wehcat_e2e_metastore_db
export CONF_BACKUP=/Users/${USER}/tmp