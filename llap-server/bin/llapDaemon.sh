#!/usr/bin/env bash

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


# Runs a yarn command as a daemon.
#
# Environment Variables
#
#   LLAP_DAEMON_HOME - Directory with jars 
#   LLAP_DAEMON_BIN_HOME - Directory with binaries
#   LLAP_DAEMON_CONF_DIR Conf dir for llap-daemon-site.xml
#   LLAP_DAEMON_LOG_DIR - defaults to /tmp
#   LLAP_DAEMON_PID_DIR   The pid files are stored. /tmp by default.
#   LLAP_DAEMON_NICENESS The scheduling priority for daemons. Defaults to 0.
##

#set -x

usage="Usage: llapDaemon.sh  (start|stop) "

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

# get arguments
startStop=$1
shift


rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ "${LLAP_DAEMON_CONF_DIR}" = "" ] ; then
  echo "LLAP_DAEMON_CONF_DIR must be specified"
  exit 1
fi

if [ -f "${LLAP_DAEMON_CONF_DIR}/llap-daemon-env.sh" ] ; then
  . "${LLAP_DAEMON_CONF_DIR}/llap-daemon-env.sh"
fi

# get log directory
if [ "$LLAP_DAEMON_LOG_DIR" = "" ]; then
  export LLAP_DAEMON_LOG_DIR="/tmp/llapDaemonLogs"
fi

if [ ! -w "$LLAP_DAEMON_LOG_DIR" ] ; then
  mkdir -p "$LLAP_DAEMON_LOG_DIR"
  chown $USER $LLAP_DAEMON_LOG_DIR
fi

if [ "$LLAP_DAEMON_PID_DIR" = "" ]; then
  LLAP_DAEMON_PID_DIR=/tmp/$USER
fi

# some variables
LLAP_DAEMON_LOG_BASE=llap-daemon-$USER-$HOSTNAME
export LLAP_DAEMON_LOG_FILE=$LLAP_DAEMON_LOG_BASE.log
logLog=$LLAP_DAEMON_LOG_DIR/$LLAP_DAEMON_LOG_BASE.log
logOut=$LLAP_DAEMON_LOG_DIR/$LLAP_DAEMON_LOG_BASE.out
pid=$LLAP_DAEMON_PID_DIR/llap-daemon.pid 
LLAP_DAEMON_STOP_TIMEOUT=${LLAP_DAEMON_STOP_TIMEOUT:-2}

# Set default scheduling priority
if [ "$LLAP_DAEMON_NICENESS" = "" ]; then
    export LLAP_DAEMON_NICENESS=0
fi

case $startStop in

  (start)

    [ -w "$LLAP_DAEMON_PID_DIR" ] || mkdir -p "$LLAP_DAEMON_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo llapdaemon running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    #rotate_log $logLog
    #rotate_log $logOut
    echo starting llapdaemon, logging to $logLog and $logOut
    export LLAP_DAEMON_LOGFILE=${LLAP_DAEMON_LOG_BASE}.log
    nohup nice -n $LLAP_DAEMON_NICENESS "$LLAP_DAEMON_BIN_HOME"/runLlapDaemon.sh run  >> "$logOut" 2>&1 < /dev/null &
    echo $! > $pid
    ;;
          
  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping llapDaemon
        kill $TARGET_PID
        sleep $LLAP_DAEMON_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "llapDaemon did not stop gracefully after $LLAP_DAEMON_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no llapDaemon to stop
      fi
      rm -f $pid
    else
      echo no llapDaemon to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


