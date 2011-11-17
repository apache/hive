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

does_jvm_support_ti(){
  version=$( java -version 2>&1 | awk -F '"' '/version/ {print $2}')
  if [[ "$version" < "1.5" ]]; then
      return 1
  else
      return 0
  fi
}

set_debug_param(){
  OIFS=$IFS
  IFS='='
  pair=($1)
  case "${pair[0]}" in
      recursive)
      export HIVE_DEBUG_RECURSIVE="${pair[1]}"
      ;;
      port)
      port="address=${pair[1]}"
      ;;
      mainSuspend)
      main_suspend="suspend=${pair[1]}"
      ;;
      childSuspend)
      child_suspend="suspend=${pair[1]}"
      ;;
      swapSuspend)
      tmp=$child_suspend
      child_suspend=$main_suspend
      main_suspend=$tmp
      ;;
      *)
      ;;
  esac
  IFS=$OIFS;
}

parse_debug(){
  IFS=':'
  read -ra params <<< "$1"
  IFS=','
  for param in ${params[1]}; do
    set_debug_param "$param"
  done
  unset IFS
}

set_debug_defaults(){
  export HIVE_DEBUG_RECURSIVE="y"
  port="address=8000"
  main_suspend="suspend=y"
  child_suspend="suspend=n"
}

get_debug_params(){
  set_debug_defaults
  parse_debug $1

  # For Debug -XX:+UseParallelGC is needed, as it is a (unfortunately not perfect)
  # workaround for JVM 6862295 bug, that affects some JVMs still in use
  if does_jvm_support_ti; then
    export HIVE_MAIN_CLIENT_DEBUG_OPTS=" -XX:+UseParallelGC -agentlib:jdwp=transport=dt_socket,server=y,$port,$main_suspend"
    export HIVE_CHILD_CLIENT_DEBUG_OPTS=" -XX:+UseParallelGC -agentlib:jdwp=transport=dt_socket,server=y,$child_suspend"
  else
    export HIVE_MAIN_CLIENT_DEBUG_OPTS=" -XX:+UseParallelGC -Xdebug -Xrunjdwp:transport=dt_socket,server=y,$port,$main_suspend"
    export HIVE_CHILD_CLIENT_DEBUG_OPTS=" -XX:+UseParallelGC -Xdebug -Xrunjdwp:transport=dt_socket,server=y,$child_suspend"
  fi
}

debug_help(){
  echo
  echo "Allows to debug Hive by connecting to it via JDI API"
  echo
  echo "Usage: hive --debug[:comma-separated parameters list]"
  echo
  echo "Parameters:"
  echo
  echo "recursive=<y|n>             Should child JVMs also be started in debug mode. Default: y"
  echo "port=<port_number>          Port on which main JVM listens for debug connection. Default: 8000"
  echo "mainSuspend=<y|n>           Should main JVM wait with execution for the debugger to connect. Default: y"
  echo "childSuspend=<y|n>          Should child JVMs wait with execution for the debugger to connect. Default: n"
  echo "swapSuspend                 Swaps suspend options between main and child JVMs"
  echo
}
