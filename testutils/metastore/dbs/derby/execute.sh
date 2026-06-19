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

# This script executes all hive metastore upgrade scripts on an specific
# database server in order to verify that upgrade scripts are working
# properly.

cd $(dirname $1)

echo "####################################################"
echo "Executing script for Derby SQL: $1"
echo "####################################################"

JAVA_PATH=($(readlink /etc/alternatives/java))
JAVA_PATH=`dirname $JAVA_PATH`

export DERBY_HOME=/usr/share/javadb
export JAVA_HOME=$JAVA_PATH/..
export PATH=$PATH:$DERBY_HOME/bin:$JAVA_HOME/bin
export CLASSPATH=$CLASSPATH:$DERBY_HOME/lib/derby.jar:$DERBY_HOME/lib/derbytools.jar:$DERBY_HOME/lib/derbyclient.jar

echo "connect 'jdbc:derby:/tmp/hive_hms_testing;create=true';" > /tmp/derbyRun.sql
echo "run '$1';" >> /tmp/derbyRun.sql
echo "quit;" >> /tmp/derbyRun.sql

ij /tmp/derbyRun.sql
