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

# Cleanup existing installation + configuration.
echo "####################################################"
echo "Detecting any existing Oracle XE installation:"
echo "####################################################"

apt-get clean

HTTPS_INFO=($(dpkg -l apt-transport-https | grep ^i | tr -s ' '))
if [[ ${HTTPS_INFO[1]} == "apt-transport-https" ]]
then
  echo "apt-transport-https package installed"
else
  echo "apt-transport-https package not installed"
  apt-get install -y --force-yes apt-transport-https
fi

INSTALL_INFO=($(dpkg -l oracle\* | grep ^i | tr -s ' '))

if [[ ${INSTALL_INFO[1]} == "oracle-xe" ]] && [[ ${INSTALL_INFO[2]}  == 10.2* ]]
then
  echo "Oracle XE already installed...Skipping"
else
  echo "Oracle XE not installed or is of a different version"
  apt-get purge -y --force-yes oracle-xe || /bin/true
  echo "####################################################"
  echo "Installing Oracle XE dependencies:"
  echo "####################################################"
  if grep -q "deb http://oss.oracle.com/debian unstable main non-free" /etc/apt/sources.list.d/oracle-xe.list
  then
    echo "Sources already listed"
  else
    echo "deb http://oss.oracle.com/debian unstable main non-free" > /etc/apt/sources.list.d/oracle-xe.list
  fi

  wget http://oss.oracle.com/el4/RPM-GPG-KEY-oracle  -O- | sudo apt-key add - || /bin/true
  apt-get update || /bin/true
  ls -al /var/cache/apt/archives
  apt-get install -y --force-yes oracle-xe:i386
fi

echo "####################################################"
echo "Configuring Oracle XE Environment:"
echo "####################################################"
echo "8080" > /tmp/silent.properties
echo "1521" >> /tmp/silent.properties
echo "hivepw" >> /tmp/silent.properties
echo "hivepw" >> /tmp/silent.properties
echo "y" >> /tmp/silent.properties

/etc/init.d/oracle-xe configure < /tmp/silent.properties > /tmp/silentInstall.log || /bin/true

export ORACLE_HOME=/usr/lib/oracle/xe/app/oracle/product/10.2.0/server
export PATH=$PATH:$ORACLE_HOME/bin

echo "####################################################"
echo "Setting up user account and Table space:"
echo "####################################################"
echo "drop user hiveuser cascade;" > /tmp/oraInit.sql
echo "alter database default tablespace SYSTEM;" >> /tmp/oraInit.sql
echo "drop tablespace hive_tbspace including contents and datafiles;" >> /tmp/oraInit.sql

echo "create user hiveuser identified by hivepw;" >> /tmp/oraInit.sql
echo "grant connect to hiveuser;" >> /tmp/oraInit.sql
echo "grant create table to hiveuser;" >> /tmp/oraInit.sql
echo "create smallfile tablespace hive_tbspace datafile 'hive.dbf' size 100m;" >> /tmp/oraInit.sql
echo "alter database default tablespace hive_tbspace;" >> /tmp/oraInit.sql
echo "alter user hiveuser quota 100m on hive_tbspace;" >> /tmp/oraInit.sql
echo "exit;" >> /tmp/oraInit.sql

sqlplus -L SYSTEM/hivepw@XE @/tmp/oraInit.sql

echo "DONE!!!"
