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

THISSERVICE=help
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

help() {
  echo "Usage ./hive <parameters> --service serviceName <service parameters>"
  echo "Service List: $SERVICE_LIST"
  echo "Parameters parsed:"
  echo "  --auxpath : Auxiliary jars "
  echo "  --config : Hive configuration directory"
  echo "  --service : Starts specific service/component. cli is default"
  echo "Parameters used:"
  echo "  HADOOP_HOME or HADOOP_PREFIX : Hadoop install directory"
  echo "  HIVE_OPT : Hive options"
  echo "For help on a particular service:"
  echo "  ./hive --service serviceName --help"
  echo "Debug help:  ./hive --debug --help"
}

help_help(){
  help
}

