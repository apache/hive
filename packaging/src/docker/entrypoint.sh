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

: ${DB_DRIVER:=derby}

function ensure_initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  if [ "$(echo "$HIVE_VER" | cut -d '.' -f1)" -lt "4" ]; then
    COMMAND="-${SCHEMA_COMMAND:-initSchema}"
  fi
  # Run the schematool script and capture its output
  output=$(/"$HIVE_HOME"/hive/bin/schematool --verbose -dbType $DB_DRIVER "$COMMAND" 2>&1)

  # Check if the output contains the specified conditions
  INIT_COMPLETED_STR='Initialization script completed'
  ALREADY_INIT_HAS_COMPLETED_STR='Error: ERROR: relation "BUCKETING_COLS" already exists'

  if [[ $output == *"$INIT_COMPLETED_STR"* || $output == *"$ALREADY_INIT_HAS_COMPLETED_STR"* ]]; then
    echo "Completed Condition: '$INIT_COMPLETED_STR' or '$ALREADY_INIT_HAS_COMPLETED_STR' found in the output."
  else
    echo "Failed Condition: The output does not contain '$INIT_COMPLETED_STR' nor '$ALREADY_INIT_HAS_COMPLETED_STR'."
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

# handles schema initialization
ensure_initialize_hive

if [ "${SERVICE_NAME}" == "hiveserver2" ]; then
  export HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH
elif [ "${SERVICE_NAME}" == "metastore" ]; then
  export METASTORE_PORT=${METASTORE_PORT:-9083}
fi

exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service $SERVICE_NAME
