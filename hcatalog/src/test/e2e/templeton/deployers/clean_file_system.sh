#!/usr/bin/env bash
#
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



# This script deletes things from DFS that may have been left over from previous runs of e2e
# tests to make sure next run starts with a clean slate.

. ./env.sh

echo "Deleting artifacts from HDFS..."

${HADOOP_HOME}/bin/hdfs dfs -rm -r       /user/hive/ /user/${USER}/ /user/templeton /apps /tmp /sqoopoutputdir
${HADOOP_HOME}/bin/hdfs dfs -mkdir -p    /tmp/hadoop-${USER} /user/hive/warehouse /user/${USER}/ /user/templeton /apps/templeton/jdbc /tmp/hadoop-yarn /tmp/templeton_test_out
${HADOOP_HOME}/bin/hdfs dfs -chmod -R a+rwx /user /tmp/
${HADOOP_HOME}/bin/hdfs dfs -chmod g+rwx   /user/hive/warehouse
