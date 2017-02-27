#!/usr/bin/env bash 

set -x

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Environment Variables
#   LLAP_DAEMON_HOME
#   LLAP_DAEMON_USER_CLASSPATH
#   LLAP_DAEMON_HEAPSIZE - MB
#   LLAP_DAEMON_OPTS - additional options
#   LLAP_DAEMON_LOG_LEVEL - default is INFO
#   LLAP_DAEMON_LOGGER - default is console
#   LLAP_DAEMON_LOG_DIR - defaults to /tmp
#   LLAP_DAEMON_TMP_DIR - defaults to /tmp
#   LLAP_DAEMON_LOG_FILE - 
#   LLAP_DAEMON_CONF_DIR

function print_usage() {
  echo "Usage: llap-daemon.sh [COMMAND]"
  echo "Commands: "
  echo "  classpath             print classpath"
  echo "  run                   run the daemon"
}

# if no args specified, show usage
if [ $# = 0 ]; then
  print_usage
  exit 1
fi

# get arguments
COMMAND=$1
shift


JAVA=$JAVA_HOME/bin/java
LOG_LEVEL_DEFAULT="INFO"
LOGGER_DEFAULT="console"
JAVA_OPTS_BASE="-server -Djava.net.preferIPv4Stack=true -XX:NewRatio=8 -XX:+UseNUMA -XX:+PrintGCDetails -verbose:gc -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=4 -XX:GCLogFileSize=100M -XX:+PrintGCDateStamps"

if [ ! -d "${LLAP_DAEMON_HOME}" ]; then
  echo No LLAP_DAEMON_HOME set, or is not a directory. 
  echo Please specify it in the environment.
  exit 1
fi

if [ ! -d "${LLAP_DAEMON_CONF_DIR}" ]; then
  echo No LLAP_DAEMON_CONF_DIR set, or is not a directory. 
  echo Please specify it in the environment.
  exit 1
fi

if [ ! -n "${LLAP_DAEMON_LOGGER}" ]; then
  echo "LLAP_DAEMON_LOGGER not defined... using default: ${LOGGER_DEFAULT}"
  LLAP_DAEMON_LOGGER=${LOGGER_DEFAULT}
fi

if [ ! -n "${LLAP_DAEMON_LOG_LEVEL}" ]; then
  echo "LLAP_DAEMON_LOG_LEVEL not defined... using default: ${LOG_LEVEL_DEFAULT}"
  LLAP_DAEMON_LOG_LEVEL=${LOG_LEVEL_DEFAULT}
fi

CLASSPATH=${LLAP_DAEMON_CONF_DIR}:${LLAP_DAEMON_HOME}/lib/*:${LLAP_DAEMON_HOME}/lib/tez/*:${LLAP_DAEMON_HOME}/lib/udfs/*:.

if [ -n "LLAP_DAEMON_USER_CLASSPATH" ]; then
  CLASSPATH=${CLASSPATH}:${LLAP_DAEMON_USER_CLASSPATH}
fi

if [ ! -n "${LLAP_DAEMON_LOG_DIR}" ]; then
  echo "LLAP_DAEMON_LOG_DIR not defined. Using default"
  LLAP_DAEMON_LOG_DIR="/tmp/llapDaemonLogs"
fi

if [ "$LLAP_DAEMON_LOGFILE" = "" ]; then
  LLAP_DAEMON_LOG_FILE='llapdaemon.log'
fi

if [ "$LLAP_DAEMON_HEAPSIZE" = "" ]; then
  LLAP_DAEMON_HEAPSIZE=4096
fi

if [ -n "$LLAP_DAEMON_LD_PATH" ]; then
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LLAP_DAEMON_LD_PATH
fi

# Figure out classes based on the command

if [ "$COMMAND" = "classpath" ] ; then
  echo $CLASSPATH
  exit
elif [ "$COMMAND" = "run" ] ; then
  CLASS='org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon'
fi

JAVA_OPTS_BASE="${JAVA_OPTS_BASE} -Xloggc:${LLAP_DAEMON_LOG_DIR}/gc.log"
LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} ${JAVA_OPTS_BASE}"

# Set the default GC option if none set
if [[ ! "$LLAP_DAEMON_OPTS" =~ \+Use[^[:space:]]+GC ]]
then
  LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -XX:+UseParallelGC"
fi

# In general, avoid using the OS temporary directory
if [ -n "$LLAP_DAEMON_TMP_DIR" ]; then
  export LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Djava.io.tmpdir=$LLAP_DAEMON_TMP_DIR"
fi

LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Dlog4j.configurationFile=llap-daemon-log4j2.properties"
LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Dllap.daemon.log.dir=${LLAP_DAEMON_LOG_DIR}"
LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Dllap.daemon.log.file=${LLAP_DAEMON_LOG_FILE}"
LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Dllap.daemon.root.logger=${LLAP_DAEMON_LOGGER}"
LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS} -Dllap.daemon.log.level=${LLAP_DAEMON_LOG_LEVEL}"

exec "$JAVA" -Dproc_llapdaemon -Xms${LLAP_DAEMON_HEAPSIZE}m -Xmx${LLAP_DAEMON_HEAPSIZE}m ${LLAP_DAEMON_OPTS} -classpath "$CLASSPATH" $CLASS "$@"


