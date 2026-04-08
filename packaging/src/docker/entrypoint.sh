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

set -x

# =========================================================================
# DYNAMIC JAR LOADER (AWS/S3 Support)
# =========================================================================
STAGING_DIR="/tmp/ext-jars"

# Checks if /tmp/ext-jars is mounted (via Docker volume).
if [ -d "$STAGING_DIR" ]; then
  if ls "$STAGING_DIR"/*.jar 1> /dev/null 2>&1; then
    echo "--> Copying custom jars from volume to Hive..."
    cp -vf "$STAGING_DIR"/*.jar "${HIVE_HOME}/lib/"
  else
    echo "--> Volume mounted at $STAGING_DIR, but no jars found."
  fi
fi

# =========================================================================
# REPLACE ${VARS} in the template
# =========================================================================
export HIVE_WAREHOUSE_PATH="${HIVE_WAREHOUSE_PATH:-/opt/hive/data/warehouse}"
export HIVE_SCRATCH_DIR="${HIVE_SCRATCH_DIR:-/opt/hive/scratch}"
export HIVE_QUERY_RESULTS_CACHE_DIRECTORY="${HIVE_WAREHOUSE_PATH:-/opt/hive/scratch/_resultscache_}"

envsubst < $HIVE_HOME/conf/core-site.xml.template > $HIVE_HOME/conf/core-site.xml
envsubst < $HIVE_HOME/conf/hive-site.xml.template > $HIVE_HOME/conf/hive-site.xml
# =========================================================================

: "${DB_DRIVER:=derby}"

SKIP_SCHEMA_INIT="${IS_RESUME:-false}"
[[ $VERBOSE = "true" ]] && VERBOSE_MODE="--verbose"

function initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  if [ "$(echo "$HIVE_VER" | cut -d '.' -f1)" -lt "4" ]; then
     COMMAND="-${SCHEMA_COMMAND:-initSchema}"
  fi
  if [[ -n "$VERBOSE_MODE" ]]; then
    "$HIVE_HOME/bin/schematool" -dbType "$DB_DRIVER" "$COMMAND" "$VERBOSE_MODE"
  else
    "$HIVE_HOME/bin/schematool" -dbType "$DB_DRIVER" "$COMMAND"
  fi
  if [ $? -eq 0 ]; then
    echo "Initialized Hive Metastore Server schema successfully.."
  else
    echo "Hive Metastore Server schema initialization failed!"
    exit 1
  fi
}

function run_llap {
  export HIVE_ZOOKEEPER_QUORUM="${HIVE_ZOOKEEPER_QUORUM:-zookeeper:2181}"
  export HIVE_LLAP_DAEMON_SERVICE_HOSTS="${HIVE_LLAP_DAEMON_SERVICE_HOSTS:-@llap0}"
  export LLAP_MEMORY_MB="${LLAP_MEMORY_MB:-1024}"
  export LLAP_EXECUTORS="${LLAP_EXECUTORS:-1}"

  envsubst < "$HIVE_HOME/conf/llap-daemon-site.xml.template" > "$HIVE_HOME/conf/llap-daemon-site.xml"

  export LLAP_DAEMON_LOG_DIR="${LLAP_DAEMON_LOG_DIR:-/tmp/llapDaemonLogs}"
  export LLAP_DAEMON_TMP_DIR="${LLAP_DAEMON_TMP_DIR:-/tmp/llapDaemonTmp}"
  export LOCAL_DIRS="${LOCAL_DIRS:-/tmp/llap-local}"
  mkdir -p "${LLAP_DAEMON_LOG_DIR}" "${LLAP_DAEMON_TMP_DIR}" "${LOCAL_DIRS}"

  # runLlapDaemon.sh expects jars under ${LLAP_DAEMON_HOME}/lib.
  # In this image, LLAP jars are under ${HIVE_HOME}/lib.
  export LLAP_DAEMON_HOME="${LLAP_DAEMON_HOME:-$HIVE_HOME}"
  export LLAP_DAEMON_CONF_DIR="${LLAP_DAEMON_CONF_DIR:-$HIVE_CONF_DIR}"
  export LLAP_DAEMON_USER_CLASSPATH="${LLAP_DAEMON_USER_CLASSPATH:-$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/tools/lib/*}"

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

  if [[ -n "${LLAP_EXTRA_OPTS:-}" ]]; then
    export LLAP_DAEMON_OPTS="${LLAP_DAEMON_OPTS:-} ${LLAP_EXTRA_OPTS}"
  fi

  export LLAP_DAEMON_OPTS

  LLAP_RUN_SCRIPT="${HIVE_HOME}/scripts/llap/bin/runLlapDaemon.sh"
  if [ ! -x "${LLAP_RUN_SCRIPT}" ]; then
    echo "LLAP daemon launcher script not found at ${LLAP_RUN_SCRIPT}."
    exit 1
  fi
  exec "${LLAP_RUN_SCRIPT}" run "$@"
}

export HIVE_CONF_DIR=$HIVE_HOME/conf
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
  export TEZ_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"
if [[ "${SKIP_SCHEMA_INIT}" == "false" && ( "${SERVICE_NAME}" == "hiveserver2" || "${SERVICE_NAME}" == "metastore" ) ]]; then
  # handles schema initialization
  initialize_hive
fi

if [ "${SERVICE_NAME}" == "hiveserver2" ]; then
  export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH"
  exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp --service "$SERVICE_NAME"
elif [ "${SERVICE_NAME}" == "metastore" ]; then
  export METASTORE_PORT=${METASTORE_PORT:-9083}
  if [[ -n "$VERBOSE_MODE" ]]; then
    exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp "$VERBOSE_MODE" --service "$SERVICE_NAME"
  else
    exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp --service "$SERVICE_NAME"
  fi
elif [ "${SERVICE_NAME}" == "llap" ]; then
  run_llap "$@"
fi
