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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE="--profile llap" # delete all containers regardless of profile
CLEANUP_FLAG=""

for arg in "$@"; do
  case "$arg" in
    --cleanup)
      CLEANUP_FLAG="--volumes"
      ;;
    *)
      echo "Unknown option: $arg"
      exit 1
      ;;
  esac
done

if [[ -n "$CLEANUP_FLAG" ]]; then
  echo "Stopping Hive cluster and removing compose volumes"
else
  echo "Stopping Hive cluster (volumes preserved; use --cleanup to remove them)"
fi

docker compose $PROFILE down $CLEANUP_FLAG
