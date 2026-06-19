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

# Resolve our absolute path
# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
    ls=`ls -ld "$this"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
        this="$link"
    else
        this=`dirname "$this"`/"$link"
    fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`unset CDPATH; cd "$bin"; pwd`
this="$bin/$script"

# the root of the HCatalog installation
export HCAT_HOME=`dirname "$this"`/..

# Read the env file created by the install script
. $HCAT_HOME/conf/hcat-env.sh

# back ground the metastore service and record the pid

PID_FILE=${ROOT}/var/log/hcat.pid
SLEEP_TIME_AFTER_START=15

# check if service is already running, if so exit
if [ -s "$PID_FILE" ]
then
	echo "HCatalog server appears to be running.  If you are SURE it is not" \
		" remove $PID_FILE and re-run this script."
    exit 1
fi

# add mysqldb jars
for f in ${DBROOT}/mysql-connector-java-*-bin.jar; do
  AUX_CLASSPATH=${AUX_CLASSPATH}:$f
done

# add jars from lib dir
for f in ${ROOT}/lib/*.jar ; do
  AUX_CLASSPATH=${AUX_CLASSPATH}:$f
done

# echo AUX_CLASSPATH = ${AUX_CLASSPATH}
export AUX_CLASSPATH=${AUX_CLASSPATH}


export HADOOP_HOME=$HADOOP_HOME
#export HADOOP_OPTS="-Dlog4j.configurationFile=file://${ROOT}/conf/log4j2.properties"
export HADOOP_OPTS="${HADOOP_OPTS} -server -XX:+UseConcMarkSweepGC -XX:ErrorFile=${ROOT}/var/log/hcat_err_pid%p.log -Xloggc:${ROOT}/var/log/hcat_gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
export HADOOP_HEAPSIZE=2048 # 8G is better if you have it

nohup ${ROOT}/bin/hive --service metastore >${ROOT}/var/log/hcat.out 2>${ROOT}/var/log/hcat.err &

PID=$!

if [ "${PID}x" == "x" ] ; then # we failed right off
    echo "Metastore startup failed, see $ROOT/var/log/hcat.err"
    exit 1
fi

echo Started metastore server init, testing if initialized correctly...
sleep $SLEEP_TIME_AFTER_START

if ps -p $PID > /dev/null 
then
    echo $PID > $PID_FILE
    echo "Metastore initialized successfully."
else
    echo "Metastore startup failed, see $ROOT/var/log/hcat.err"
    exit 1
fi

