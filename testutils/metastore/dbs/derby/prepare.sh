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

export DEBIAN_FRONTEND=noninteractive
OS_VERSION=`lsb_release -c | cut -d":" -f2`

echo "####################################################"
echo "Begin for OS version $OS_VERSION"
echo "####################################################"

HTTPS_INFO=($(dpkg -l apt-transport-https | grep ^i | tr -s ' '))
if [[ ${HTTPS_INFO[1]} == "apt-transport-https" ]]
then
  echo "apt-transport-https package installed"
else
  echo "apt-transport-https package not installed"
  apt-get install -y --force-yes apt-transport-https
fi

INSTALL_INFO=($(dpkg -l \*javadb-core\* | grep ^ii | tr -s ' '))

if [[ ${INSTALL_INFO[1]} == "sun-javadb-core" ]]
then
  echo "Derby already installed...Skipping"
else
  echo "Derby not installed"
  # Cleanup existing installation + configuration.
  apt-get purge -y --force-yes derby-tools sun-javadb-client sun-javadb-core sun-javadb-common libderby-java openjdk-7-jre openjdk-7-jre openjdk-7-jre-headless || /bin/true
  echo "####################################################"
  echo "Installing Derby dependencies:"
  echo "####################################################"
  apt-get update || /bin/true
  apt-get install -y --force-yes -o Dpkg::Options::="--force-overwrite" sun-javadb-core sun-javadb-client derby-tools
fi

JAVA_PATH=($(readlink /etc/alternatives/java))
JAVA_PATH=`dirname $JAVA_PATH`

export DERBY_HOME=/usr/share/javadb
export JAVA_HOME=$JAVA_PATH/..
export PATH=$PATH:/usr/share/javadb/bin:$JAVA_HOME/bin
export CLASSPATH=$CLASSPATH:$DERBY_HOME/lib/derby.jar:$DERBY_HOME/lib/derbytools.jar:$DERBY_HOME/lib/derbyclient.jar
rm -rf /tmp/hive_hms_testing;

echo "connect 'jdbc:derby:/tmp/hive_hms_testing;create=true';" > /tmp/derbyInit.sql
ij /tmp/derbyInit.sql 

echo "DONE!!!"

