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

THISSERVICE=hwi
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hwi() {

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  CLASS=org.apache.hadoop.hive.hwi.HWIServer
  # The ls hack forces the * to be expanded which is required because 
  # System.getenv doesn't do globbing
  export HWI_JAR_FILE=$(ls ${HIVE_LIB}/hive-hwi-*.jar)
  export HWI_WAR_FILE=$(ls ${HIVE_LIB}/hive-hwi-*.war)

  #hwi requires ant jars
  if [ "$ANT_LIB" = "" ] ; then
    ANT_LIB=/opt/ant/lib
  fi
  for f in ${ANT_LIB}/*.jar; do
    if [[ ! -f $f ]]; then
      continue;
    fi
    HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
  done

  export HADOOP_CLASSPATH
  
  # hadoop 20 or newer - skip the aux_jars option and hiveconf
  exec $HADOOP jar ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@"
}

hwi_help(){
  echo "Usage ANT_LIB=XXXX hive --service hwi"	
}
