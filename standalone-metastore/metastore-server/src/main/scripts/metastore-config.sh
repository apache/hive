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

#
# processes --config option from command line
#

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
# bin is set from calling script
script=`basename "$this"`
this="$bin/$script"

# the root of the Hive installation
if [[ -z $METASTORE_HOME ]] ; then
  export METASTORE_HOME=`dirname "$bin"`
fi

#check to see if the conf dir is given as an optional argument
while [ $# -gt 0 ]; do    # Until you run out of parameters . . .
  case "$1" in
    --config)
        shift
        confdir=$1
        shift
        METASTORE_CONF_DIR=$confdir
        ;;
    --auxpath)
        shift
        METASTORE_AUX_JARS_PATH=$1
        shift
        ;;
    *)
        break;
        ;;
  esac
done


# Allow alternate conf dir location.
METASTORE_CONF_DIR="${METASTORE_CONF_DIR:-$METASTORE_HOME/conf}"

export METASTORE_CONF_DIR=$METASTORE_CONF_DIR
export METASTORE_AUX_JARS_PATH=$METASTORE_AUX_JARS_PATH

# Default to use 256MB 
export HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-256}
