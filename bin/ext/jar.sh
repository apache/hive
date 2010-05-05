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

THISSERVICE=jar
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

jar () {
  RUNJAR=$1
  shift

  RUNCLASS=$1
  shift

  if [ -z "$RUNJAR" ] ; then
    echo "RUNJAR not specified"
    exit 3
  fi

  if [ -z "$RUNCLASS" ] ; then
    echo "RUNCLASS not specified"
    exit 3
  fi

  if [ $minor_ver -lt 20 ]; then
    exec $HADOOP jar $AUX_JARS_CMD_LINE $RUNJAR $RUNCLASS $HIVE_OPTS "$@"
  else
    # hadoop 20 or newer - skip the aux_jars option and hiveconf
    exec $HADOOP jar $RUNJAR $RUNCLASS $HIVE_OPTS "$@"
  fi
}

jar_help () {
  echo "Used for applications that require Hadoop and Hive classpath and environment."
  echo "./hive --service jar <yourjar> <yourclass> HIVE_OPTS <your_args>"
}
