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

THISSERVICE=schemaTool
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

export HADOOP_CLIENT_OPTS=" --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED  --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.regex=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED $HADOOP_CLIENT_OPTS "

schemaTool() {
  METASTORE_OPTS=''
  CLASS=org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool
  if $cygwin; then
    METASTORE_LIB=`cygpath -w "$METASTORE_LIB"`
  fi
  JAR=${METASTORE_LIB}/hive-standalone-metastore-server-*.jar

  # Prevent Log4j2 from being reconfigured after initialization
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j2.disableShutdownHook=true"
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.shutdownHookEnabled=false"

  # CRITICAL: Tell Hadoop to load metastore jars FIRST to prevent SLF4J binding conflicts
  export HADOOP_USER_CLASSPATH_FIRST=true

  # Filter HADOOP_CLASSPATH to remove old SLF4J 1.x bindings
  if [ "$HADOOP_CLASSPATH" != "" ]; then
    FILTERED_CP=""
    IFS=':' read -ra CP_ARRAY <<< "$HADOOP_CLASSPATH"
    for cp_entry in "${CP_ARRAY[@]}"; do
      # Skip SLF4J 1.x binding jars by filename
      if [[ "$(basename "$cp_entry")" == slf4j-log4j12-*.jar || \
            "$(basename "$cp_entry")" == slf4j-reload4j-*.jar || \
            "$(basename "$cp_entry")" == log4j-slf4j-impl-*.jar || \
            "$(basename "$cp_entry")" == reload4j-*.jar ]]; then
        continue
      fi
      # Skip explicit SLF4J 1.x jar paths
      if [[ "$cp_entry" == *slf4j-log4j12* || \
            "$cp_entry" == *slf4j-reload4j* || \
            "$cp_entry" == */reload4j-* ]]; then
        continue
      fi
      if [ "$FILTERED_CP" == "" ]; then
        FILTERED_CP="$cp_entry"
      else
        FILTERED_CP="${FILTERED_CP}:${cp_entry}"
      fi
    done
    export HADOOP_CLASSPATH="$FILTERED_CP"
  fi

  # hadoop 20 or newer - skip the aux_jars option and hiveconf
  exec $HADOOP jar $JAR $CLASS "$@"
}

schemaTool_help () {
  schemaTool -h
}
