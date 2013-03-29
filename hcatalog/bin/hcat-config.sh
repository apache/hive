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

# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

# the root of the HCATALOG installation
export HCAT_PREFIX=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      HCAT_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
if [ -e "${HCAT_PREFIX}/etc/hcatalog/hcat-env.sh" ]; then
  DEFAULT_CONF_DIR=${HCAT_PREFIX}/"etc/hcatalog"
elif [ -e "${HCAT_PREFIX}/conf/hcat-env.sh" ]; then
  DEFAULT_CONF_DIR=${HCAT_PREFIX}/"conf"
else
  DEFAULT_CONF_DIR="/etc/hcatalog"
fi
HCAT_CONF_DIR="${HCAT_CONF_DIR:-$DEFAULT_CONF_DIR}"

#users can add various env vars to hcat-env.sh in the conf
#rather than having to export them before running the command
if [ -f "${HCAT_CONF_DIR}/hcat-env.sh" ]; then
  . "${HCAT_CONF_DIR}/hcat-env.sh"
fi

#determine where hadoop is
#check HADOOP_HOME and then check HADOOP_PREFIX
if [ -f ${HADOOP_HOME}/bin/hadoop ]; then
  HADOOP_PREFIX=$HADOOP_HOME
#if this is an rpm install check for /usr/bin/hadoop
elif [ -f ${HCAT_PREFIX}/bin/hadoop ]; then
  HADOOP_PREFIX=$HCAT_PREFIX
#otherwise see if HADOOP_PREFIX is defined
elif [ ! -f ${HADOOP_PREFIX}/bin/hadoop ]; then
  echo "Hadoop not found."
  exit 1
fi
