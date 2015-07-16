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

THISSERVICE=hplsql
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hplsql () {
  CLASS=org.apache.hive.hplsql.Hplsql;

  # include only the HPL/SQL jar and its dependencies
  hplsqlJarPath=`ls ${HIVE_LIB}/hive-hplsql-*.jar`
  antlrJarPath="${HIVE_LIB}/antlr-runtime-4.5.jar"
  hadoopClasspath=""
  if [[ -n "${HADOOP_CLASSPATH}" ]]
  then
    hadoopClasspath="${HADOOP_CLASSPATH}:"
  fi
  export HADOOP_CLASSPATH="${hadoopClasspath}${HIVE_CONF_DIR}:${hplsqlJarPath}:${antlrJarPath}"

  exec $HADOOP jar ${hplsqlJarPath} $CLASS $HIVE_OPTS "$@"
}

hplsql_help () {
  hplsql "--help"
} 
