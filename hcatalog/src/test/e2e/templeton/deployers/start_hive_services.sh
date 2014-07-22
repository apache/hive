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

#This script copies precanned *-site.xml files need to start Hive and WebHCat services, then
#starts the services


source ./env.sh

#decide which DB to run against
cp ${PROJ_HOME}/hcatalog/src/test/e2e/templeton/deployers/config/hive/hive-site.xml ${HIVE_HOME}/conf/hive-site.xml
#cp ${PROJ_HOME}/hcatalog/src/test/e2e/templeton/deployers/config/hive/hive-site.mssql.xml ${HIVE_HOME}/conf/hive-site.xml

cp ${PROJ_HOME}/hcatalog/src/test/e2e/templeton/deployers/config/webhcat/webhcat-site.xml ${HIVE_HOME}/hcatalog/etc/webhcat/webhcat-site.xml

if [ -d ${WEBHCAT_LOG_DIR} ]; then
  rm -Rf ${WEBHCAT_LOG_DIR};
fi
mkdir -p ${WEBHCAT_LOG_DIR};
echo "Starting Metastore..."
nohup ${HIVE_HOME}/bin/hive --service metastore -p9933 >>${WEBHCAT_LOG_DIR}/metastore_console.log 2>>${WEBHCAT_LOG_DIR}/metastore_error.log &
echo $! > ${WEBHCAT_LOG_DIR}/metastore.pid
echo "Starting WebHCat..."
${HIVE_HOME}/hcatalog/sbin/webhcat_server.sh start

jps;
