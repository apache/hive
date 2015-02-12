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


#This script copies files needed by e2e tests to DFS

source ./env.sh

echo "Deploying artifacts to HDFS..."

${HADOOP_HOME}/bin/hdfs dfs -put ${PROJ_HOME}/hcatalog/src/test/e2e/templeton/inpdir/ webhcate2e
#For hadoop1 we copy the same file with 2 names
#$HADOOP_HOME/bin/hadoop fs -put hadoop-examples-1.2.1.jar  webhcate2e/hexamples.jar
#$HADOOP_HOME/bin/hadoop fs -put hadoop-examples-1.2.1.jar  webhcate2e/hclient.jar

#For hadoop2 there are 2 separate jars
${HADOOP_HOME}/bin/hdfs dfs -put ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar  webhcate2e/hexamples.jar
${HADOOP_HOME}/bin/hdfs dfs -put ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}.jar webhcate2e/hclient.jar
${HADOOP_HOME}/bin/hdfs dfs -put ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-${HADOOP_VERSION}.jar  /user/templeton/hadoop-streaming.jar

#must match config/webhcat/webhcat-stie.xml
${HADOOP_HOME}/bin/hdfs dfs -put ${PROJ_HOME}/packaging/target/apache-hive-${HIVE_VERSION}-bin.tar.gz /apps/templeton/apache-hive-${HIVE_VERSION}-bin.tar.gz
# To run against Hadoop2 cluster, you have to build Pig tar yourself with 
# "ant -Dforrest.home=$FORREST_HOME -Dhadoopversion=23 clean tar"
${HADOOP_HOME}/bin/hadoop fs -put ${PIG_TAR_PATH}/pig-${PIG_VERSION}.tar.gz /apps/templeton/pig-${PIG_VERSION}.tar.gz
${HADOOP_HOME}/bin/hadoop fs -put ${PIG_PIGGYBANK_PATH}  webhcate2e/
#standard Pig distro from ASF for Hadoop 1
# ${HADOOP_HOME}/bin/hadoop fs -put /Users/ekoifman/dev/data/jarsForTmplte2e/pig-0.12.0.tar.gz /apps/templeton/pig-0.12.0.tar.gz
#${HADOOP_HOME}/bin/hadoop fs -put /Users/ekoifman/dev/data/jarsForTmplte2e/pig-0.12.0/contrib/piggybank/java/piggybank.jar  webhcate2e/


${HADOOP_HOME}/bin/hadoop fs -put /Users/ekoifman/dev/sqoop-1.4.5.bin__hadoop-2.0.4-alpha.tar.gz /apps/templeton/sqoop-1.4.5.bin__hadoop-2.0.4-alpha.tar.gz
${HADOOP_HOME}/bin/hadoop fs -put /Users/ekoifman/dev/mysql-connector-java-5.1.30/mysql-connector-java-5.1.30-bin.jar /apps/templeton/jdbc/mysql-connector-java.jar

#Tez set up (http://tez.apache.org/install.html)
#if not using Tez - ignore this
${HADOOP_HOME}/bin/hdfs dfs -put /Users/ekoifman/dev/apache-tez-${TEZ_VERSION}-src/tez-dist/target/tez-${TEZ_VERSION}.tar.gz /apps/tez-${TEZ_VERSION}.tar.gz
${HADOOP_HOME}/bin/hdfs dfs -mkdir /tmp/tezin
${HADOOP_HOME}/bin/hdfs dfs -mkdir /tmp/tezout
${HADOOP_HOME}/bin/hdfs dfs -put /Users/ekoifman/dev/hive/build.sh /tmp/tezin
#Above line is for Sanity Check: this is to run #6 in http://tez.apache.org/install.html
#$HADOOP_HOME/bin/hadoop jar tez-examples-0.5.3.jar orderedwordcount /tmp/tezin /tmp/tezout



#check what got deployed
${HADOOP_HOME}/bin/hdfs dfs -ls -R /apps webhcate2e /user/templeton /user/hive/warehouse
