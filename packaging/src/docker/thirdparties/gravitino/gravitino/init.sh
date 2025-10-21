#!/bin/bash

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

# Copy Gravitino start script
cp /tmp/gravitino/start-iceberg-rest-server.sh /root/gravitino-iceberg-rest-server/bin/start-iceberg-rest-server.sh

# Copy Gravitino config file
cp /tmp/gravitino/gravitino-iceberg-rest-server.conf /root/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-server.conf

# Download H2 Driver to Gravitino libs folder
mkdir -p /root/gravitino-iceberg-rest-server/libs
curl -L -o /root/gravitino-iceberg-rest-server/libs/h2-2.2.220.jar https://repo1.maven.org/maven2/com/h2database/h2/2.2.220/h2-2.2.220.jar

/bin/bash /root/gravitino-iceberg-rest-server/bin/start-iceberg-rest-server.sh