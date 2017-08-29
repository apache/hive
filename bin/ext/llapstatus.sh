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

THISSERVICE=llapstatus
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

llapstatus () {
  CLASS=org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver;
  if [ ! -f ${HIVE_LIB}/hive-cli-*.jar ]; then
    echo "Missing Hive CLI Jar"
    exit 3;
  fi

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  set -e;

  export HADOOP_CLIENT_OPTS=" -Dproc_llapstatuscli $HADOOP_CLIENT_OPTS -Dlog4j.configurationFile=llap-cli-log4j2.properties "
  # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
  $HADOOP $CLASS $HIVE_OPTS "$@"
  
}

llapstatus_help () {
  CLASS=org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver;
  execHiveCmd $CLASS "--help"
} 

