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

if [ -f /tmp/metastore_db.zstd ]; then
  echo "The following tar command might print warnings (leading '/' and no such file or directory)."
  echo "The cause is due to the way the dump was created. Please ignore these warnings."
  zstdcat /tmp/metastore_db.zstd | tar -C /var/lib/postgresql/ -x
  rm /tmp/metastore_db.zstd
fi

/usr/local/bin/docker-entrypoint.sh "$@"

