#!/bin/sh -x

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

apk add --no-cache acl

mkdir -p /tmp/hive/jars
mkdir -p $WAREHOUSE
chmod 777 $WAREHOUSE

# Give the hive user id full rwx access to all existing files and directories under $WAREHOUSE
setfacl -R -m u:$HIVE_USER_ID:rwx $WAREHOUSE

# Ensure all new files/directories created inside $WAREHOUSE automatically grant rwx access to hive user id
setfacl -d -m u:$HIVE_USER_ID:rwx $WAREHOUSE
