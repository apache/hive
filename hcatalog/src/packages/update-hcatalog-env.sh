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

  Optional installation parameters:
     --arch=i386                 OS Architecture
     --bin-dir=PREFIX/bin        Executable directory
     --conf-dir=/etc/hcatalog    Configuration directory
     --lib-dir=/PREFIX/lib       Library directory
     --log-dir=/var/log/hcatalog Log directory
     --pid-dir=/var/run          PID file location
     --sbin-dir=PREFIX/sbin      System executable directory
     --java-home=/opt/jre-home   JDK or JRE JAVA_HOME

     --proto-env=/path/to/hcat-env.sh.template
                                 path to hcat-env.sh template
     --proto-site=/path/to/proto-hive-site.xml
                                 path to hive-site.xml template
     --output-env=/path/to/hcat-env.sh
                                 path to file to write out to, in case we
                                 don't want to replace the installed hcat-env.sh
                                 (used only if not specifying --install)
     --output-site=/path/to/hive-site.xml
                                 path to file to write out to, in case we
                                 don't want to replace the installed hive-site.xml
                                 (used only if not specifying --install)

  Optional hcatalog parameters:
     --sasl-enabled              Specify if we're using Secure Hadoop
     --metastore-port=9933       Server port the metastore runs on

  Optional hcatalog client parameters:
     --metastore-server=localhost
                                 Hostname of server on which the metastore server is running

  Optional hcatalog-server parameters:
     --user=hcat                 User that hcat-server runs as
     --warehouse-dir=/user/hcat/warehouse
                                 HDFS path to the hive warehouse dir
     --dbhost=DBHOST             Hostname of server on which database server is setup
     --dbname=hcatmetastoredb    Database name
     --dbjars=/opt/dbjars/       JDBC connector driver directory
     --dbdriver=com.mysql.jdbc.Driver
                                 JDBC connector driver to use
     --dbproto=mysql             JDBC protocol to talk to driver
     --dbuser=DBUSER             Database user for hcat-server to use
     --dbpasswd=DBPASSWORD       Database password for hcat-server to use
     --keytab-path=/etc/security/keytab
                                 Location of keytab (used only if sasl-enabled is specified)
     --kerberos-principal=PRINCIPAL
                                 Kerberos Principal for metastore server to use
     --kerberos-realm=REALM      Kerberos Principal autocalculated as hcat/_HOST@REALM
                                 (used only if --kerneros-principal is not specified)

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
  -l 'prefix:' \
  -l 'arch:' \
  -l 'bin-dir:' \
  -l 'conf-dir:' \
  -l 'lib-dir:' \
  -l 'log-dir:' \
  -l 'pid-dir:' \
  -l 'sbin-dir:' \
  -l 'java-home:' \
  -l 'proto-env:' \
  -l 'proto-site:' \
  -l 'output-env:' \
  -l 'output-site:' \
  -l 'sasl-enabled' \
  -l 'metastore-port:' \
  -l 'metastore-server:' \
  -l 'user:' \
  -l 'warehouse-dir:' \
  -l 'dbjars:' \
  -l 'dbproto:' \
  -l 'dbhost:' \
  -l 'dbname:' \
  -l 'dbdriver:' \
  -l 'dbuser:' \
  -l 'dbpasswd:' \
  -l 'keytab-path:' \
  -l 'kerberos-principal:' \
  -l 'kerberos-realm:' \
  -l 'uninstall' \
  -l 'install' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "${OPTS}"
while true ; do
  case "$1" in
    --prefix)
      PREFIX=$2 ; shift 2
      ;;
    --arch)
      ARCH=$2 ; shift 2
      ;;
    --bin-dir)
      BIN_DIR=$2 ; shift 2
      ;;
    --conf-dir)
      CONF_DIR=$2 ; shift 2
      ;;
    --lib-dir)
      LIB_DIR=$2 ; shift 2
      ;;
    --log-dir)
      LOG_DIR=$2 ; shift 2
      ;;
    --pid-dir)
      PID_DIR=$2 ; shift 2
      ;;
    --sbin-dir)
      SBIN_DIR=$2 ; shift 2
      ;;
    --java-home)
      JAVA_HOME=$2 ; shift 2
      ;;
    --proto-env)
      PROTO_ENV=$2 ; shift 2
      ;;
    --proto-site)
      PROTO_SITE=$2 ; shift 2
      ;;
    --output-env)
      OUTPUT_ENV=$2 ; shift 2
      ;;
    --output-site)
      OUTPUT_SITE=$2 ; shift 2
      ;;
    --sasl-enabled)
      PARAM_SASL_ENABLED="true"; shift
      ;;
    --metastore-port)
      PARAM_METASTORE_PORT=$2 ; shift 2
      ;;
    --metastore-server)
      PARAM_METASTORE_SERVER=$2 ; shift 2
      ;;
    --user)
      SVR_USER=$2 ; shift 2
      ;;
    --warehouse-dir)
      SVR_WHDIR=$2 ; shift 2
      ;;
    --dbhost)
      SVR_DBHOST=$2 ; shift 2
      ;;
    --dbname)
      SVR_DBNAME=$2 ; shift 2
      ;;
    --dbdriver)
      SVR_DBDRIVER=$2 ; shift 2
      ;;
    --dbproto)
      SVR_DBPROTO=$2 ; shift 2
      ;;
    --dbjars)
      SVR_DBJARS=$2 ; shift 2
      ;;
    --dbuser)
      SVR_DBUSER=$2 ; shift 2
      ;;
    --dbpasswd)
      SVR_DBPASSWD=$2 ; shift 2
      ;;
    --keytab-path)
      SVR_KEYTAB_PATH=$2 ; shift 2
      ;;
    --kerberos-principal)
      SVR_KERBEROS_PRINCIPAL=$2 ; shift 2
      ;;
    --kerberos-realm)
      SVR_KERBEROS_REALM=$2 ; shift 2
      ;;
    --uninstall)
      UNINSTALL=1; shift
      # echo [--uninstall] Disabled in script mode
      # usage
      # exit 1
      ;;
    --install)
      INSTALL=1; shift
      # echo [--install] Disabled in script mode
      # usage
      # exit 1
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

HCAT_PROTO_ENV=${PROTO_ENV:-$HCAT_PREFIX/share/hcatalog/templates/conf/hcat-env.sh.template}
HCAT_PROTO_SITE=${PROTO_SITE:-$HCAT_PREFIX/share/hcatalog/templates/conf/hive-site.xml.template}

SASL_ENABLED=${PARAM_SASL_ENABLED:-false}
METASTORE_PORT=${PARAM_METASTORE_PORT:-9933}

METASTORE_SERVER=${PARAM_METASTORE_SERVER:-`uname -n`}

USER=${SVR_USR:-hcat}
WHDIR=${SVR_WHDIR:-/WAREHOUSE_DIR}
DBHOST=${SVR_DBHOST:-localhost}
DBNAME=${SVR_DBNAME:-hcatmetastoredb}
DBJARS=${SVR_DBJARS:-/opt/dbjars}
DBPROTO=${SVR_DBPROTO:-mysql}
DBDRIVER=${SVR_DBDRIVER:-com.mysql.jdbc.Driver}
DBUSER=${SVR_DBUSER:-DBUSER}
DBPASSWD=${SVR_DBPASSWD:-DBPASSWD}
KEYTAB_PATH=${SVR_KEYTAB_PATH:-KEYTAB_PATH}

KERBEROS_REALM=${SVR_KERBEROS_REALM:-REALM}
KERBEROS_PRINCIPAL=${SVR_KERBEROS_PRINCIPAL:-hcat/$METASTORE_SERVER@$KERBEROS_REALM}


UNINSTALL=${UNINSTALL:-0}
INSTALL=${INSTALL:-0}

if [ $INSTALL == "0" ]; then
  HCAT_OUTPUT_ENV=${OUTPUT_ENV:-$HCAT_CONF_DIR/hcat-env.sh}
  HCAT_OUTPUT_SITE=${OUTPUT_SITE:-$HCAT_CONF_DIR/hive-site.xml}
else
  # If in install mode, ignore --output-env and --output-site. we write to the conf dir
  HCAT_OUTPUT_ENV=$HCAT_CONF_DIR/hcat-env.sh
  HCAT_OUTPUT_SITE=$HCAT_CONF_DIR/hcat-site.sh
fi

if [ "${ARCH}" != "i386" ]; then
  HCAT_LIB_DIR=${HCAT_LIB_DIR}64
fi

if [ -z "${JAVA_HOME}" ]; then
  if [ -e /etc/debian_version ]; then
    JAVA_HOME=/usr/lib/jvm/java-6-sun/jre
  else
    JAVA_HOME=/usr/java/default
  fi
fi

if [ $UNINSTALL == "1" ]; then
  # Remove symlinks
  if [ "${HCAT_CONF_DIR}" != "${HCAT_PREFIX}/etc/hcatalog" ]; then
    rm -rf ${HCAT_PREFIX}/etc/hcatalog
  fi
  rm -f /etc/default/hcat-env.sh
  rm -f /etc/profile.d/hcat-env.sh
  echo Uninstall preparation done.
  exit 0
fi

if [ $INSTALL == "1" ]; then
  # Create symlinks
  if [ "${HCAT_CONF_DIR}" != "${HCAT_PREFIX}/etc/hcatalog" ]; then
    mkdir -p ${HCAT_PREFIX}/etc
    ln -sf ${HCAT_CONF_DIR} ${HCAT_PREFIX}/etc/hcatalog
  fi
  ln -sf ${HCAT_CONF_DIR}/hcat-env.sh /etc/default/hcat-env.sh
  ln -sf ${HCAT_CONF_DIR}/hcat-env.sh /etc/profile.d/hcat-env.sh

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
fi

TFILE1="/tmp/$(basename $0).$$.1.tmp"
TFILE2="/tmp/$(basename $0).$$.2.tmp"
template_generator $HCAT_PROTO_ENV $TFILE1
template_generator $HCAT_PROTO_SITE $TFILE2
cp ${TFILE1} ${HCAT_OUTPUT_ENV} ; chown hcat:hadoop ${HCAT_OUTPUT_ENV} ; chmod 775 ${HCAT_OUTPUT_ENV}
cp ${TFILE2} ${HCAT_OUTPUT_SITE} ; chown hcat:hadoop ${HCAT_OUTPUT_SITE} ; chmod 755 ${HCAT_OUTPUT_SITE}
rm -f ${TFILE1}
rm -f ${TFILE2}
