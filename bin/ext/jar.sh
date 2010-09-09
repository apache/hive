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

THISSERVICE=jar
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

jar () {
  RUNJAR=$1
  shift

  RUNCLASS=$1
  shift

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  version=$($HADOOP version | awk '{if (NR == 1) {print $2;}}');

  # Save the regex to a var to workaround quoting incompatabilities
  # between Bash 3.1 and 3.2
  version_re="^([[:digit:]]+)\.([[:digit:]]+)(\.([[:digit:]]+))?.*$"

  if [[ "$version" =~ $version_re ]]; then
      major_ver=${BASH_REMATCH[1]}
      minor_ver=${BASH_REMATCH[2]}
      patch_ver=${BASH_REMATCH[4]}
  else
      echo "Unable to determine Hadoop version information."
      echo "'hadoop version' returned:"
      echo `$HADOOP version`
      exit 6
  fi

  if [ -z "$RUNJAR" ] ; then
    echo "RUNJAR not specified"
    exit 3
  fi

  if [ -z "$RUNCLASS" ] ; then
    echo "RUNCLASS not specified"
    exit 3
  fi

  if [ $minor_ver -lt 20 ]; then
    exec $HADOOP jar $AUX_JARS_CMD_LINE $RUNJAR $RUNCLASS $HIVE_OPTS "$@"
  else
    # hadoop 20 or newer - skip the aux_jars option and hiveconf
    exec $HADOOP jar $RUNJAR $RUNCLASS $HIVE_OPTS "$@"
  fi
}

jar_help () {
  echo "Used for applications that require Hadoop and Hive classpath and environment."
  echo "./hive --service jar <yourjar> <yourclass> HIVE_OPTS <your_args>"
}
