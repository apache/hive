#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Support functions
#

# Follow symlinks on Linux and Darwin
function real_script_name() {
        local base=$1
        local real
        if readlink -f $base >/dev/null 2>&1; then
                # Darwin/Mac OS X
                real=`readlink -f $base`
        fi
        if [[ "$?" != "0" || -z "$real" ]]; then
                # Linux
                local bin=$(cd -P -- "$(dirname -- "$base")">/dev/null && pwd -P)
                local script="$(basename -- "$base")"
                real="$bin/$script"
        fi
        echo "$real"
}

function usage() {
        echo "usage: $0 [start|stop|foreground]"
        echo "  start           Start the Webhcat Server"
        echo "  stop            Stop the Webhcat Server"
        echo "  foreground      Run the Webhcat Server in the foreground"
        exit 1
}

# Print an error message and exit
function die() {
        echo "webhcat: $@" 1>&2
        exit 1
}

# Print an message
function log() {
        echo "webhcat: $@"
}

# return(print) the webhcat jar
function find_jar_path() {
         for dir in "." "build" "share/webhcat/svr/lib"; do
                if (( `ls -1 $base_dir/$dir/$WEBHCAT_JAR 2>/dev/null| wc -l ` > 1 )) ; then
                       echo "Error:  found more than one hcatalog jar in $base_dir/$dir/$WEBHCAT_JAR"
                       exit 1
                fi
                if (( `ls -1 $base_dir/$dir/$WEBHCAT_JAR 2>/dev/null | wc -l` == 0 )) ; then
                       continue
                fi
 
                local jar=`ls $base_dir/$dir/$WEBHCAT_JAR`
                if [[ -f $jar ]]; then
                       echo $jar
                       break
                fi
        done
}

# Find the webhcat classpath
function find_classpath() {
        local classpath=""
        for dir in  "share/webhcat/svr/" "share/webhcat/svr/lib/"  "conf" ; do
                local path="$base_dir/$dir"

                if [[ -d $path ]]; then
                        for jar_or_conf in $path/*; do
                                if [[ -z "$classpath" ]]; then
                                        classpath="$jar_or_conf"
                                else
                                        classpath="$classpath:$jar_or_conf"
                                fi
                        done
                fi
        done

        if [[ -n "$WEBHCAT_CONF_DIR" ]]; then
                if [[ -z "$classpath" ]]; then
                        classpath="$WEBHCAT_CONF_DIR"
                else
                        classpath="$classpath:$WEBHCAT_CONF_DIR"
                fi
        fi

        # Append hcat classpath
        local hcat_classpath
        hcat_classpath=`$base_dir/bin/hcat -classpath`
        if [[ "$?" != "0" ]]; then
                die "Unable to get the hcatalog classpath"
        fi
        echo "$classpath:$hcat_classpath"
}

# Check if the pid is running
function check_pid() {
        local pid=$1
        if ps -p $pid > /dev/null; then
                return 0
        else
                return 1
        fi
}

# Start the webhcat server in the foreground
function foreground_webhcat() {
        exec $start_cmd
}

# Start the webhcat server in the background.  Record the PID for
# later use.
function start_webhcat() {
        if [[ -f $PID_FILE ]]; then
                # Check if there is a server running
                local pid=`cat $PID_FILE`
                if check_pid $pid; then
                        die "already running on process $pid"
                fi
        fi

        log "starting ..."
        log "$start_cmd"
        nohup $start_cmd >>$CONSOLE_LOG 2>>$ERROR_LOG &
        local pid=$!

        if [[ -z "${pid}" ]] ; then # we failed right off
                die "failed to start. Check logs in " `dirname $ERROR_LOG`
        fi

        sleep $SLEEP_TIME_AFTER_START

        if check_pid $pid; then
                echo $pid > $PID_FILE
                log "starting ... started."
        else
                die "failed to start. Check logs in " `dirname $ERROR_LOG`
        fi
}

# Stop a running server
function stop_webhcat() {
        local pid
        if [[ -f $PID_FILE ]]; then
                # Check if there is a server running
                local check=`cat $PID_FILE`
                if check_pid $check; then
                        pid=$check
                fi
        fi

        if [[ -z "$pid" ]]; then
                log "no running server found"
        else
                log "stopping ..."
                kill $pid
                sleep $SLEEP_TIME_AFTER_START
                if check_pid $pid; then
                        die "failed to stop"
                else
                        log "stopping ... stopped"
                fi
        fi
}

#
# Build command line and run
#

this=`real_script_name "${BASH_SOURCE-$0}"`
this_bin=`dirname $this`
base_dir="$this_bin/.."

if [[ -f "$base_dir/libexec/webhcat_config.sh" ]]; then
        . "$base_dir/libexec/webhcat_config.sh"
else
        . "$this_bin/webhcat_config.sh"
fi

JAR=`find_jar_path`
if [[ -z "$JAR" ]]; then
        die "No webhcat jar found"
fi

CLASSPATH=`find_classpath`
if [[ -z "$CLASSPATH" ]]; then
        die "No classpath or jars found"
fi
CLASSPATH="$JAR:$CLASSPATH"

if [[ -z "$HADOOP_CLASSPATH" ]]; then
        export HADOOP_CLASSPATH="$CLASSPATH"
else
        export HADOOP_CLASSPATH="$CLASSPATH:$HADOOP_CLASSPATH"
fi

if [[ -z "$WEBHCAT_LOG4J" ]]; then
  WEBHCAT_LOG4J="file://$base_dir/etc/webhcat/webhcat-log4j.properties";
fi

export HADOOP_USER_CLASSPATH_FIRST=true
export HADOOP_OPTS="${HADOOP_OPTS} -Dwebhcat.log.dir=$WEBHCAT_LOG_DIR -Dlog4j.configuration=$WEBHCAT_LOG4J"

start_cmd="$HADOOP_PREFIX/bin/hadoop jar $JAR org.apache.hive.hcatalog.templeton.Main  "


cmd=$1
case $cmd in
        start)
                start_webhcat
                ;;
        stop)
                stop_webhcat
                ;;
        foreground)
                foreground_webhcat
                ;;
        *)
                usage
                ;;
esac

log "done"
