#!/bin/bash

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

set -euo pipefail
set -x

export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export HIVE_HOME=${HIVE_HOME:-/opt/hive}
export TEZ_HOME=${TEZ_HOME:-/opt/tez}

# Allow external configuration directories to be mounted in for Hadoop/Tez.
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export TEZ_CONF_DIR="${TEZ_CONF_DIR:-$HADOOP_CONF_DIR}"
export HIVE_CONF_DIR="$HIVE_HOME/conf"

# =========================================================================
# REPLACE ${VARS} in the template
# =========================================================================
: "${HIVE_WAREHOUSE_PATH:=/opt/hive/data/warehouse}"
: "${HIVE_SCRATCH_DIR:=/opt/hive/scratch}"
: "${HIVE_QUERY_RESULTS_CACHE_DIRECTORY:=/opt/hive/scratch/_resultscache_}"
export HIVE_WAREHOUSE_PATH
export HIVE_SCRATCH_DIR
export HIVE_QUERY_RESULTS_CACHE_DIRECTORY

envsubst < $HIVE_HOME/conf/core-site.xml.template > $HIVE_HOME/conf/core-site.xml
envsubst < $HIVE_HOME/conf/hive-site.xml.template > $HIVE_HOME/conf/hive-site.xml
envsubst < $HIVE_HOME/conf/llap-daemon-site.xml.template > $HIVE_HOME/conf/llap-daemon-site.xml
# =========================================================================


# LLAP daemon logging, temp, and local dirs; can be overridden by the user.
export LLAP_DAEMON_LOG_DIR="${LLAP_DAEMON_LOG_DIR:-/tmp/llapDaemonLogs}"
export LLAP_DAEMON_TMP_DIR="${LLAP_DAEMON_TMP_DIR:-/tmp/llapDaemonTmp}"
export LOCAL_DIRS="${LOCAL_DIRS:-/tmp/llap-local}"

mkdir -p "${LLAP_DAEMON_LOG_DIR}" "${LLAP_DAEMON_TMP_DIR}" "${LOCAL_DIRS}"

# runLlapDaemon.sh expects jars under ${LLAP_DAEMON_HOME}/lib.
# In the Docker image, LLAP jars are under ${HIVE_HOME}/lib.
export LLAP_DAEMON_HOME="${LLAP_DAEMON_HOME:-$HIVE_HOME}"
export LLAP_DAEMON_CONF_DIR="${LLAP_DAEMON_CONF_DIR:-$HIVE_CONF_DIR}"
export LLAP_DAEMON_USER_CLASSPATH="${LLAP_DAEMON_USER_CLASSPATH:-$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*}"

JAVA_ADD_OPENS=(
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.util=ALL-UNNAMED"
  "--add-opens=java.base/java.io=ALL-UNNAMED"
  "--add-opens=java.base/java.net=ALL-UNNAMED"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
  "--add-opens=java.base/java.util.regex=ALL-UNNAMED"
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
  "--add-opens=java.sql/java.sql=ALL-UNNAMED"
  "--add-opens=java.base/java.text=ALL-UNNAMED"
  "-Dnet.bytebuddy.experimental=true"
)

for opt in "${JAVA_ADD_OPENS[@]}"; do
  if [[ " ${LLAP_DAEMON_OPTS:-} " != *" ${opt} "* ]]; then
    LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS:-} ${opt}"
  fi
done
export LLAP_DAEMON_OPTS

# Let callers supply extra opts without overriding LLAP defaults.
if [[ -n "${LLAP_EXTRA_OPTS:-}" ]]; then
  export LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS:-} ${LLAP_EXTRA_OPTS}"
fi

LLAP_RUN_SCRIPT="${HIVE_HOME}/scripts/llap/bin/runLlapDaemon.sh"
if [ ! -x "${LLAP_RUN_SCRIPT}" ]; then
  echo "LLAP daemon launcher script not found at ${LLAP_RUN_SCRIPT}."
  exit 1
fi

exec "${LLAP_RUN_SCRIPT}" run "$@"

