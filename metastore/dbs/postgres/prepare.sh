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
echo "$OS_VERSION"

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

INSTALL_INFO=($(dpkg -l postgresql-9.4\* | grep ^i | tr -s ' '))

if [[ ${INSTALL_INFO[1]} == "postgresql-9.4" ]]
then
  echo "PostgreSQL already installed...Skipping"
else
  echo "PostgreSQL not installed"
  # Cleanup existing installation + configuration.
  apt-get purge -y --force-yes postgressql-9.4 || /bin/true
  echo "####################################################"
  echo "Installing PostgreSQL dependencies:"
  echo "####################################################"
  if grep -q "deb http://apt.postgresql.org/pub/repos/apt/ $OS_VERSION-pgdg main" /etc/apt/sources.list.d/postgreSQL.list
  then
    echo "Sources already listed"
  else
    echo "deb http://apt.postgresql.org/pub/repos/apt/ $OS_VERSION-pgdg main" >> /etc/apt/sources.list.d/postgreSQL.list
  fi

  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
  apt-get update  || /bin/true
  apt-get install -y --force-yes postgresql-9.4
fi

echo "####################################################"
echo "Configuring PostgreSQL Environment:"
echo "####################################################"
echo "drop database if exists hive_hms_testing;" > /tmp/postgresInit.sql
echo "drop user if exists hiveuser;" >> /tmp/postgresInit.sql
echo "create user hiveuser createdb createuser password 'hivepw';" >> /tmp/postgresInit.sql
echo "create database hive_hms_testing owner hiveuser;" >> /tmp/postgresInit.sql
sudo -u postgres psql -f /tmp/postgresInit.sql

echo "DONE!!!"

