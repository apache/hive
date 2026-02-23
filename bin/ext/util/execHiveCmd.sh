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

CLI_JAR="hive-cli-*.jar"
BEELINE_JAR="hive-beeline-*.jar"

execHiveCmd () {
  CLASS=$1;
  shift;

  # if jar is not passed as parameter use corresponding cli jar
  if [ "$1" == "$CLI_JAR" ] || [ "$1" == "$BEELINE_JAR" ]; then
    JAR="$1"
    shift;
  else
    if [ "$USE_DEPRECATED_CLI" == "true" ]; then
      JAR="$CLI_JAR"
    else
      JAR="$BEELINE_JAR"
    fi
  fi

  # cli specific code
  if [ ! -f ${HIVE_LIB}/$JAR ]; then
    echo "Missing $JAR Jar"
    exit 3;
  fi

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  # For services that may encounter SLF4J conflicts (schemaTool, schematool, beeline),
  # filter out old SLF4J 1.x jars from HADOOP_CLASSPATH to prevent binding conflicts
  if [[ "$SERVICE" =~ ^(schemaTool|schematool|beeline)$ ]]; then
    # [FIX]: Prefer packaged Hive configs first so CI does not depend on host /etc/hive content.
    if [ -f "${HIVE_HOME}/conf/hive-log4j2.properties" ]; then
      export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j2.configurationFile=file://${HIVE_HOME}/conf/hive-log4j2.properties"
    # Packaged dist may only ship the template; materialize a .properties file so Log4j2
    # selects the PropertiesConfiguration parser (it infers parser from filename extension).
    elif [ -f "${HIVE_HOME}/conf/hive-log4j2.properties.template" ]; then
      HIVE_LOG4J2_DIR="$(mktemp -d -t hive-log4j2.XXXXXX)"
      HIVE_LOG4J2_FILE="${HIVE_LOG4J2_DIR}/hive-log4j2.properties"
      cp "${HIVE_HOME}/conf/hive-log4j2.properties.template" "${HIVE_LOG4J2_FILE}"
      export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j2.configurationFile=file://${HIVE_LOG4J2_FILE}"
    # Only fall back to system default if packaged configs are missing
    elif [ -f "${HIVE_CONF_DIR}/hive-log4j2.properties" ]; then
      export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j2.configurationFile=file://${HIVE_CONF_DIR}/hive-log4j2.properties"
    fi

    # CRITICAL: Tell Hadoop to load Hive's jars FIRST, overriding globally-installed
    # Hadoop/Tez jars that contain old SLF4J 1.x bindings. This prevents the multiple
    # SLF4J binding conflict seen in CI environments.
    export HADOOP_USER_CLASSPATH_FIRST=true

    # Filter HADOOP_CLASSPATH to remove paths with old SLF4J/log4j bindings
    if [ "$HADOOP_CLASSPATH" != "" ]; then
      FILTERED_CP=""
      IFS=':' read -ra CP_ARRAY <<< "$HADOOP_CLASSPATH"
      for cp_entry in "${CP_ARRAY[@]}"; do
        # Skip only specific SLF4J 1.x binding jars by filename
        if [[ "$(basename "$cp_entry")" == slf4j-log4j12-*.jar || \
              "$(basename "$cp_entry")" == slf4j-reload4j-*.jar || \
              "$(basename "$cp_entry")" == log4j-slf4j-impl-*.jar ]]; then
          continue
        fi
        # Skip explicit SLF4J 1.x jar paths
        if [[ "$cp_entry" == *slf4j-log4j12* || "$cp_entry" == *slf4j-reload4j* ]]; then
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
  fi

  # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
  exec $HADOOP jar ${HIVE_LIB}/$JAR $CLASS $HIVE_OPTS "$@"
}
