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

TOOLS_LIB="${HADOOP_HOME}/share/hadoop/tools/lib"
COMMON_LIB="${HADOOP_HOME}/share/hadoop/common/lib"

# Checks if /tmp/ext-jars is mounted (via Docker volume).
if [ -d "$STAGING_DIR" ]; then
  # Check for aws-java-sdk-bundle (Wildcard handles versions)
  if ls "$STAGING_DIR"/aws-java-sdk-bundle-*.jar 1> /dev/null 2>&1; then
    echo "--> Installing AWS SDK Bundle..."
    cp "$STAGING_DIR"/aws-java-sdk-bundle-*.jar "$COMMON_LIB/"
    echo "--> activating hadoop-aws from tools..."
    cp "$TOOLS_LIB"/hadoop-aws-*.jar "$COMMON_LIB/"
  fi
fi

: "${DB_DRIVER:=derby}"

SKIP_SCHEMA_INIT="${IS_RESUME:-false}"
[[ $VERBOSE = "true" ]] && VERBOSE_MODE="--verbose" || VERBOSE_MODE=""

function initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  # Check Hive version. If < 4.0.0, use older initSchema command
  if [ "$(echo "$HIVE_VER" | cut -d '.' -f1)" -lt "4" ]; then
     COMMAND="-${SCHEMA_COMMAND:-initSchema}"
  fi

  "$HIVE_HOME/bin/schematool" -dbType "$DB_DRIVER" "$COMMAND" "$VERBOSE_MODE"
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
fi

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"

if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  # handles schema initialization
  initialize_hive
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}

echo "Starting Hive Metastore on port $METASTORE_PORT..."
exec "$HIVE_HOME/bin/start-metastore"
