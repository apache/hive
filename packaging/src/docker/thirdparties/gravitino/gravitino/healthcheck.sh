#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -ex

max_attempts=3
attempt=0
success=false

while [ $attempt -lt $max_attempts ]; do
  response=$(curl -X GET -H "Content-Type: application/json" http://gravitino:9001/iceberg/api/version)
  
  if echo "$response" | grep -q "HTTP ERROR 401 The provided credentials did not support" 
  then
    success=true
    break
  else
    echo "Attempt $((attempt + 1)) failed..."
    sleep 1
  fi

  ((attempt++))
done

if [ "$success" = true ]; then
  exit 0
else
  exit 1
fi