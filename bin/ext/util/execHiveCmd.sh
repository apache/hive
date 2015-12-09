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

  # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
  exec $HADOOP jar ${HIVE_LIB}/$JAR $CLASS $HIVE_OPTS "$@"
}
