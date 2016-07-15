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




# Path to the directory which containts hive-exec and hive-llap-server
#export LLAP_DAEMON_HOME=

#Path to the directory containing llap configs. Eventually, merge with hive-site.
#export LLAP_DAEMON_CONF_DIR=

# Heap size in MB for the llap-daemon. Determined by #executors, #memory-per-executor setup in llap-daemon-configuration.
#export LLAP_DAEMON_HEAPSIZE=

# Path to the BIN scripts. Ideally this should be the same as the hive bin directories.
#export LLAP_DAEMON_BIN_HOME=

# Set this to a path containing tez jars
#export LLAP_DAEMON_USER_CLASSPATH=

# Logger setup for LLAP daemon
#export LLAP_DAEMON_LOGGER=query-routing

# Log level for LLAP daemon
#export LLAP_DAEMON_LOG_LEVEL=INFO

# Directory to which logs will be generated
#export LLAP_DAEMON_LOG_DIR=

# Directory in which the pid file will be generated
#export LLAP_DAEMON_PID_DIR=

# Additional JAVA_OPTS for the daemon process
#export LLAP_DAEMON_OPTS=
