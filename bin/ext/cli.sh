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

THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

# Set old CLI as the default client
if [ -n '$USE_DEPRECATED_CLI' ]; then
  USE_DEPRECATED_CLI="true"
fi

updateBeelineOpts() {
  # If process is backgrounded, don't change terminal settings
  if [[ ( ! $(ps -o stat= -p $$) =~ *+ ) && ! ( -p /dev/stdin ) ]]; then
    export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djline.terminal=jline.UnsupportedTerminal"
  fi
}

updateCli() {
  if [ "$USE_DEPRECATED_CLI" == "true" ]; then
    CLASS=org.apache.hadoop.hive.cli.CliDriver
    JAR=hive-cli-*.jar
  else
    export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dlog4j.configurationFile=beeline-log4j2.properties"
    CLASS=org.apache.hive.beeline.cli.HiveCli
    JAR=hive-beeline-*.jar
    updateBeelineOpts
  fi
}

cli () {
  updateCli
  execHiveCmd $CLASS $JAR "$@"
}

cli_help () {
  updateCli
  execHiveCmd $CLASS $JAR "--help"
}
