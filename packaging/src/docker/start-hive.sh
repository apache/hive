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

#!/usr/bin/env bash
set -e

MODE="container"
PROFILE=""
SCALE=""

for arg in "$@"; do
  case "$arg" in
    --llap)
      MODE="llap"
      PROFILE="--profile llap"
      SCALE="--scale llapdaemon=2"
      export HIVE_ZOOKEEPER_QUORUM=zookeeper:2181
      export HIVE_LLAP_DAEMON_SERVICE_HOSTS=@llap0
      ;;
    *)
      echo "Unknown option: $arg"
      exit 1
      ;;
  esac
done

export HIVE_EXECUTION_MODE="$MODE"

echo "Starting Hive cluster (mode=$HIVE_EXECUTION_MODE)"

docker compose $PROFILE up -d $SCALE

docker compose $PROFILE logs -f