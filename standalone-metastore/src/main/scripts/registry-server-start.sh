#!/bin/bash

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


if [ $# -lt 1 ];
then
	echo "USAGE: $0 [-daemon] registry.yaml"
	exit 1
fi
base_dir=$(dirname $0)/..

if [ "x$REGISTRY_HEAP_OPTS" = "x" ]; then
    export REGISTRY_HEAP_OPTS="-Xmx1G -Xms1G"
fi

EXTRA_ARGS="-name RegistryServer"

# create logs directory
if [ "x$LOG_DIR" = "x" ]; then
    LOG_DIR="$base_dir/logs"
fi

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

# classpath addition for release

echo $CLASSPATH
for file in $base_dir/lib/*.jar;
do
    CLASSPATH=$CLASSPATH:$file
done

if [ "x$EXT_CLASSPATH" = "x" ]; then
 CLASSPATH=$CLASSPATH;$EXT_CLASSPATH;
fi

COMMAND=$1
case $COMMAND in
  -name)
    DAEMON_NAME=$2
    CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
    shift 2
    ;;
  -daemon)
    DAEMON_MODE=true
    shift
    ;;
  *)
    ;;
esac

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# JVM performance options
if [ -z "$REGISTRY_JVM_PERFORMANCE_OPTS" ]; then
  REGISTRY_JVM_PERFORMANCE_OPTS="-server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

#Application classname
APP_CLASS="org.apache.hadoop.hive.registry.webservice.RegistryApplication"

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
    nohup $JAVA $REGISTRY_HEAP_OPTS $REGISTRY_JVM_PERFORMANCE_OPTS -cp $CLASSPATH $REGISTRY_OPTS "$APP_CLASS" "server" "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
    exec $JAVA $REGISTRY_HEAP_OPTS $REGISTRY_JVM_PERFORMANCE_OPTS -cp $CLASSPATH $REGISTRY_OPTS "$APP_CLASS" "server" "$@"
fi
