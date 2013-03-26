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

# This script configures hcat-env.sh and symlinkis directories for 
# relocating RPM locations.

usage() {
  echo "
usage: $0 <parameters>
  Required parameters:
     --prefix=PREFIX             path to install into

  Optional parameters:
     --arch=i386                 OS Architecture
     --bin-dir=PREFIX/bin        Executable directory
     --conf-dir=/etc/hcatalog    Configuration directory
     --log-dir=/var/log/hcatalog Log directory
     --mysql-dir=/usr/share/java MySQL connector directory
     --pid-dir=/var/run          PID file location
     --sbin-dir=PREFIX/sbin      System executable directory
  "
  exit 1
}

template_generator() {
  REGEX='(\$\{[a-zA-Z_][a-zA-Z_0-9]*\})'
  cat $1 |
  while read line ; do
    while [[ "$line" =~ $REGEX ]] ; do
      LHS=${BASH_REMATCH[1]}
      RHS="$(eval echo "\"$LHS\"")"
      line=${line//$LHS/$RHS}
    done
    echo $line >> $2
  done
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'arch:' \
  -l 'prefix:' \
  -l 'bin-dir:' \
  -l 'conf-dir:' \
  -l 'lib-dir:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'mysql-dir:' \
  -l 'sbin-dir:' \
  -l 'uninstall' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --arch)
      ARCH=$2 ; shift 2
      ;;
    --prefix)
      PREFIX=$2 ; shift 2
      ;;
    --bin-dir)
      BIN_DIR=$2 ; shift 2
      ;;
    --log-dir)
      LOG_DIR=$2 ; shift 2
      ;;
    --lib-dir)
      LIB_DIR=$2 ; shift 2
      ;;
    --conf-dir)
      CONF_DIR=$2 ; shift 2
      ;;
    --pid-dir)
      PID_DIR=$2 ; shift 2
      ;;
    --mysql-dir)
      MYSQL_DIR=$2 ; shift 2
      ;;
    --sbin-dir)
      SBIN_DIR=$2 ; shift 2
      ;;
    --uninstall)
      UNINSTALL=1; shift
      ;;
    --)
      shift ; break
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

for var in PREFIX; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

ARCH=${ARCH:-i386}
HCAT_PREFIX=$PREFIX
HCAT_BIN_DIR=${BIN_DIR:-$PREFIX/bin}
HCAT_CONF_DIR=${CONF_DIR:-$PREFIX/etc/hcatalog}
HCAT_LIB_DIR=${LIB_DIR:-$PREFIX/lib}
HCAT_LOG_DIR=${LOG_DIR:-$PREFIX/var/log}
HCAT_PID_DIR=${PID_DIR:-$PREFIX/var/run}
HCAT_SBIN_DIR=${SBIN_DIR:-$PREFIX/sbin}
DBROOT=${MYSQL_DIR:-/usr/sharejava}
UNINSTALL=${UNINSTALL:-0}

if [ "${ARCH}" != "i386" ]; then
  HCAT_LIB_DIR=${HCAT_LIB_DIR}64
fi

if [ "${UNINSTALL}" -eq "1" ]; then
  # Remove symlinks
  if [ "${HCAT_CONF_DIR}" != "${HCAT_PREFIX}/etc/hcatalog" ]; then
    rm -rf ${HCAT_PREFIX}/etc/hcatalog
  fi
  rm -f /etc/default/hcat-env.sh
  rm -f /etc/profile.d/hcat-env.sh
else
  # Create symlinks
  if [ "${HCAT_CONF_DIR}" != "${HCAT_PREFIX}/etc/hcatalog" ]; then
    mkdir -p ${HCAT_PREFIX}/etc
    ln -sf ${HCAT_CONF_DIR} ${HCAT_PREFIX}/etc/hcatalog
  fi
  ln -sf ${HCAT_CONF_DIR}/hcat-env.sh /etc/default/hcat-env.sh
  ln -sf ${HCAT_CONF_DIR}/hcat-env.sh /etc/profile.d/hcat-env.sh

  if [ ! -e "${HCAT_CONF_DIR}/proto-hive-site.xml" ]; then
    cp ${HCAT_CONF_DIR}/proto-hive-site.xml ${HCAT_CONF_DIR}/hive-site.xml
    chown hcat:hadoop ${HCAT_CONF_DIR}/hive-site.xml
    chmod 700 ${HCAT_CONF_DIR}/hive-site.xml
  fi

  mkdir -p ${HCAT_LOG_DIR}
  chown hcat:hadoop ${HCAT_LOG_DIR}
  chmod 775 ${HCAT_LOG_DIR}

  if [ ! -d ${HCAT_PID_DIR} ]; then
    mkdir -p ${HCAT_PID_DIR}
    chown hcat:hadoop ${HCAT_PID_DIR}
    chmod 775 ${HCAT_PID_DIR}
  fi

  TFILE="/tmp/$(basename $0).$$.tmp"
  if [ -z "${JAVA_HOME}" ]; then
    if [ -e /etc/debian_version ]; then
      JAVA_HOME=/usr/lib/jvm/java-6-sun/jre
    else
      JAVA_HOME=/usr/java/default
    fi
  fi
  template_generator ${HCAT_PREFIX}/share/hcatalog/templates/conf/hcat-env.sh.template $TFILE
  cp ${TFILE} ${HCAT_CONF_DIR}/hcat-env.sh
  rm -f ${TFILE}
fi
