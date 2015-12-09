#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`

HCAT_LOG_DIR=${HCAT_LOG_DIR:-"$bin"/../var/log}
HCAT_PID_DIR=${HCAT_PID_DIR:-$HCAT_LOG_DIR}

if [ -e "$bin/../libexec/hcat-config.sh" ]; then
  . "$bin"/../libexec/hcat-config.sh
else
  . "$bin"/hcat-config.sh
fi

function print_usage() {
  echo "Usage: $0 [--config confdir] COMMAND"
  echo "  start  Start HCatalog Server"
  echo "  stop   Stop HCatalog Server"
}

function start_hcat() {
  # back ground the metastore service and record the pid
  PID_FILE=${HCAT_PID_DIR}/hcat.pid
  SLEEP_TIME_AFTER_START=15

  # check if service is already running, if so exit
  if [ -s "$PID_FILE" ]
  then
    echo "HCatalog server appears to be running.  If you are SURE it is not" \
         " remove $PID_FILE and re-run this script."
    exit 1
  fi

  HIVE_SITE_XML=${HIVE_HOME}/conf/hive-site.xml
  if [ ! -e $HIVE_SITE_XML ]
  then
    echo "Missing hive-site.xml, expected at [$HIVE_SITE_XML]";
    exit 1
  fi

  # Find our Warehouse dir from the config file
#  WAREHOUSE_DIR=`sed -n '/<name>hive.metastore.warehouse.dir<\/name>/ {
#      n
#      s/.*<value>\(.*\)<\/value>.*/\1/p
#      }' $HIVE_SITE_XML`
#  HADOOP_OPTS="$HADOOP_OPTS -Dhive.metastore.warehouse.dir=$WAREHOUSE_DIR "

  # add in hive-site.xml to classpath
  AUX_CLASSPATH=${AUX_CLASSPATH}:`dirname ${HIVE_SITE_XML}`

  # add jars from db connectivity dir - be careful to not point to something like /lib
  for f in ${DBROOT}/*.jar; do
    AUX_CLASSPATH=${AUX_CLASSPATH}:$f
  done

  # add jars from lib dir
  for f in ${HCAT_PREFIX}/share/hcatalog/lib/*.jar ; do
    AUX_CLASSPATH=${AUX_CLASSPATH}:$f
  done

  for f in ${HCAT_PREFIX}/share/hcatalog/*.jar ; do
    AUX_CLASSPATH=${AUX_CLASSPATH}:$f
  done

  # echo AUX_CLASSPATH = ${AUX_CLASSPATH}
  export AUX_CLASSPATH=${AUX_CLASSPATH}

  export HADOOP_HOME=$HADOOP_HOME
  #export HADOOP_OPTS="-Dlog4j.configurationFile=file://${HCAT_PREFIX}/conf/log4j2.properties"
  export HADOOP_OPTS="${HADOOP_OPTS} -server -XX:+UseConcMarkSweepGC -XX:ErrorFile=${HCAT_LOG_DIR}/hcat_err_pid%p.log -Xloggc:${HCAT_LOG_DIR}/hcat_gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
  export HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-2048} # 8G is better if you have it
  export METASTORE_PORT=${METASTORE_PORT:-9083}
  nohup ${HIVE_HOME}/bin/hive --service metastore >${HCAT_LOG_DIR}/hcat.out 2>${HCAT_LOG_DIR}/hcat.err &

  PID=$!

  if [ "${PID}x" == "x" ] ; then # we failed right off
    echo "Metastore startup failed, see ${HCAT_LOG_DIR}/hcat.err"
    exit 1
  fi

  echo Started metastore server init, testing if initialized correctly...
  sleep $SLEEP_TIME_AFTER_START

  if ps -p $PID > /dev/null
  then
    echo $PID > $PID_FILE
    echo "Metastore initialized successfully on port[${METASTORE_PORT}]."
  else
    echo "Metastore startup failed, see ${HCAT_LOG_DIR}/hcat.err"
    exit 1
  fi
}

function stop_hcat() {
  SLEEP_TIME_AFTER_KILL=30

  PID_FILE=${HCAT_PID_DIR}/hcat.pid
  echo looking for $PID_FILE

  # check if service is already running, if so exit
  if [ -s "$PID_FILE" ] ; then
    PID=`cat $PID_FILE`
    echo "Found metastore server process $PID, killing..."
    kill $PID
    sleep $SLEEP_TIME_AFTER_KILL

    # if process is still around, use kill -9
    if ps -p $PID > /dev/null ; then
        echo "Initial kill failed, getting serious now..."
        kill -9 $PID
    fi
    if ps -p $PID > /dev/null ; then
        echo "Wow, even kill -9 failed, giving up; sorry"
        exit 1
    else
        rm -rf $PID_FILE
        echo "Successfully shutdown metastore"
    fi
  fi
}

if [ $# = 0 ]; then
  print_usage
  exit
fi

COMMAND=$1
case $COMMAND in
  start)
    start_hcat
    ;;
  stop)
    stop_hcat
    ;;
esac

