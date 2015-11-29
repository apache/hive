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
THISSERVICE=version
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

version () {
  JAR=$1
  if [ -z "$JAR" ] ; then
    JAR=${HIVE_LIB}/hive-exec-*.jar
  else
    JAR=${HIVE_LIB}/$1
  fi

  # hadoop 20 or newer - skip the aux_jars option and hiveconf
  CLASS=org.apache.hive.common.util.HiveVersionInfo
  exec $HADOOP jar $JAR $CLASS 2>> ${STDERR}
}

version_help () {
  echo "Show Version information of hive jars"
  echo "./hive --version [hiveJar]"
} 

