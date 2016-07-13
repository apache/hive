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

# Need arguments [host [port [db]]]
THISSERVICE=beeline
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

beeline () {
  CLASS=org.apache.hive.beeline.BeeLine;

  # include only the beeline client jar and its dependencies
  beelineJarPath=`ls ${HIVE_LIB}/hive-beeline-*.jar`
  superCsvJarPath=`ls ${HIVE_LIB}/super-csv-*.jar`
  jlineJarPath=`ls ${HIVE_LIB}/jline-*.jar`
  hadoopClasspath=""
  if [[ -n "${HADOOP_CLASSPATH}" ]]
  then
    hadoopClasspath="${HADOOP_CLASSPATH}:"
  fi
  export HADOOP_CLASSPATH="${hadoopClasspath}${HIVE_CONF_DIR}:${beelineJarPath}:${superCsvJarPath}:${jlineJarPath}"
  export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.configurationFile=beeline-log4j2.properties "

  exec $HADOOP jar ${beelineJarPath} $CLASS $HIVE_OPTS "$@"
}

beeline_help () {
  beeline "--help"
} 
