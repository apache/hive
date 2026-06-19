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

#====================================
#determine HADOOP_HOME if possible
#====================================
function find_hadoop_home() {
  # check for hadoop in the path
  HADOOP_IN_PATH=`which hadoop 2>/dev/null`
  if [ -f ${HADOOP_IN_PATH} ]; then
    HADOOP_DIR=`dirname "$HADOOP_IN_PATH"`/..
  fi
  # HADOOP_HOME env variable overrides hadoop in the path
  HADOOP_HOME=${HADOOP_HOME:-${HADOOP_PREFIX:-$HADOOP_DIR}}
  if [ "$HADOOP_HOME" == "" ]; then
    echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
    exit 4;
  fi

  HADOOP=$HADOOP_HOME/bin/hadoop
  if [ ! -f ${HADOOP} ]; then
    echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
    exit 4;
  fi
}
