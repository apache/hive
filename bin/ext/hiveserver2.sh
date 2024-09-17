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

THISSERVICE=hiveserver2
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

if [ "$HIVESERVER2_PID_DIR" = "" ]; then
  HIVESERVER2_PID_DIR=$HIVE_CONF_DIR
fi

HIVESERVER2_PID=$HIVESERVER2_PID_DIR/hiveserver2.pid

before_start() {
  #ckeck if the process is not running
  mkdir -p "$HIVESERVER2_PID_DIR"
  if [ -f $HIVESERVER2_PID ]; then
    if kill -0 $(cat $HIVESERVER2_PID) >/dev/null 2>&1; then
      echo "HiveServer2 running as process $(cat $HIVESERVER2_PID).  Stop it first."
      ps $(cat $HIVESERVER2_PID)
      exit 1
    fi
  fi
}

hiveserver2() {
  CLASS=org.apache.hive.service.server.HiveServer2
  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi
  JAR=${HIVE_LIB}/hive-service-[0-9].*.jar

  if [ "$1" = "--graceful_stop" ]; then
    pid=$2
    if [ "$pid" = "" -a -f $HIVESERVER2_PID ]; then
      pid=$(cat $HIVESERVER2_PID)
    fi
    TIMEOUT_KEY='hive.server2.graceful.stop.timeout'
    timeout=$(exec $HADOOP jar $JAR $CLASS $HIVE_OPTS --getHiveConf $TIMEOUT_KEY | grep $TIMEOUT_KEY'=' | awk -F'=' '{print $2}')
    killAndWait $pid $timeout
  else
    export HADOOP_CLIENT_OPTS=" -Dproc_hiveserver2 $HADOOP_CLIENT_OPTS "
    export HADOOP_OPTS="$HIVESERVER2_HADOOP_OPTS $HADOOP_OPTS"
    commands=$(exec $HADOOP jar $JAR $CLASS -H | grep -v '-hiveconf' | awk '{print $1}')
    start_hiveserver2='Y'
    for i in "$@"; do
      if [ "$(echo "${commands[@]}" | grep -we $i)" != "" ]; then
        start_hiveserver2='N'
        break
      fi
    done
    if [ "$start_hiveserver2" == "Y" ]; then
      before_start
      echo >&2 "$(timestamp): Starting HiveServer2"
      hiveserver2_pid="$$"
      echo $hiveserver2_pid > ${HIVESERVER2_PID}
    fi
    exec $HADOOP jar $JAR $CLASS $HIVE_OPTS "$@"
  fi
}

# Function to kill and wait for a process end. Take the pid and timeout as parameters
killAndWait() {
  pidToKill=$1
  timeout=$2
  processedAt=$(date +%s)
  # kill -0 == see if the PID exists
  if kill -0 $pidToKill >/dev/null 2>&1; then
    echo "$(timestamp): Stopping HiveServer2 of pid $pidToKill in $timeout seconds."
    kill $pidToKill >/dev/null 2>&1
    while kill -0 $pidToKill >/dev/null 2>&1; do
      echo -n "."
      sleep 1
      # if process persists more than $HIVESERVER2_STOP_TIMEOUT (default 1800 sec) no mercy
      if [ $(($(date +%s) - $processedAt)) -gt ${HIVESERVER2_STOP_TIMEOUT:-$timeout} ]; then
        break
      fi
    done
    echo
    # process still there : kill -9
    if kill -0 $pidToKill >/dev/null 2>&1; then
      echo "$(timestamp): Force stopping HiveServer2 with kill -9 $pidToKill"
      kill -9 $pidToKill >/dev/null 2>&1
    fi
  else
    retval=$?
    echo "No HiveServer2 to stop because kill -0 of pid $pidToKill failed with status $retval"
  fi
}

hiveserver2_help() {
  hiveserver2 -H
}

timestamp() {
  date +"%Y-%m-%d %T"
}
