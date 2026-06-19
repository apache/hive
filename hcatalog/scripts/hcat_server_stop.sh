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

PID_FILE=${ROOT}/var/log/hcat.pid
SLEEP_TIME_AFTER_KILL=30

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
	else
    	rm -rf $PID_FILE
		echo "Successfully shutdown metastore"
	fi
fi
