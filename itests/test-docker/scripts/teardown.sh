#!/bin/bash
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

set -eux

base_dir=$(dirname "$(cd "$(dirname "$0")" || exit; pwd)")

if [ -f "$base_dir/target/hive-metastore-thrift.pid" ]; then
  kill $(cat "$base_dir/target/hive-metastore-thrift.pid")
fi
if [ -f "$base_dir/target/hive-metastore-rest.pid" ]; then
  kill $(cat "$base_dir/target/hive-metastore-rest.pid")
fi
if [ -f "$base_dir/target/ozone-s3g.pid" ]; then
  kill $(cat "$base_dir/target/ozone-s3g.pid")
fi

kubectl delete -f "$base_dir/k8s/*"
helm uninstall ozone
