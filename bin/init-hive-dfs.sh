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


# The purpose of this script is to set warehouse's directories on HDFS

DEFAULT_WAREHOUSE_DIR="/user/hive/warehouse"
DEFAULT_TMP_DIR="/tmp"

WAREHOUSE_DIR=${DEFAULT_WAREHOUSE_DIR}
TMP_DIR=${DEFAULT_TMP_DIR}
HELP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --warehouse-dir)
      shift
      WAREHOUSE_DIR=$1
      shift
      ;;
    --tmp-dir)
      shift
      TMP_DIR=$1
      shift
      ;;
    --help)
      HELP=_help
      shift
      ;;
    *)
      echo "Invalid parameter: $1"
      HELP=_help
      break
      ;;
  esac
done

if [ "$HELP" = "_help" ] ; then
  echo "Usage $0 [--warehouse-dir <Hive user>] [--tmp-dir <Tmp dir>]"
  echo "Default value of warehouse directory is: [$DEFAULT_WAREHOUSE_DIR]"
  echo "Default value of the temporary directory is: [$DEFAULT_TMP_DIR]"
  exit -1
fi


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

HADOOP_EXEC=$HADOOP_HOME/bin/hadoop
if [ ! -f ${HADOOP} ]; then
  echo "Cannot find hadoop installation: \$HADOOP_HOME or \$HADOOP_PREFIX must be set or hadoop must be in the path";
  exit 4;
fi


# Ensure /tmp exist
$HADOOP_EXEC fs -test -d ${TMP_DIR} > /dev/null 2>&1
if [ $? -ne 0 ] 
then
  echo "Creating directory [${TMP_DIR}]"
  $HADOOP_EXEC fs -mkdir ${TMP_DIR}
fi

echo "Setting writeable group rights for directory [${TMP_DIR}]"
$HADOOP_EXEC fs -chmod g+w ${TMP_DIR}


# Ensure warehouse dir exist
$HADOOP_EXEC fs -test -d ${WAREHOUSE_DIR} > /dev/null 2>&1
if [ $? -ne 0 ] 
then
  echo "Creating directory [${WAREHOUSE_DIR}]"
  $HADOOP_EXEC fs -mkdir ${WAREHOUSE_DIR}
fi

echo "Setting writeable group rights for directory [${WAREHOUSE_DIR}]"
$HADOOP_EXEC fs -chmod g+w ${WAREHOUSE_DIR}

echo "Initialization done."
echo
echo "Please, do not forget to set the following configuration properties in hive-site.xml:"
echo "hive.metastore.warehouse.dir=${WAREHOUSE_DIR}"
echo "hive.exec.scratchdir=${TMP_DIR}"

exit 0
