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
: "${HIVE_WAREHOUSE_PATH:=/opt/hive/data/warehouse}"
export HIVE_WAREHOUSE_PATH

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

export HIVE_CONF_DIR=$HIVE_HOME/conf
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
  export TEZ_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"
if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
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
fi
