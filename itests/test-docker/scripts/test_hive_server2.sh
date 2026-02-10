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

kubectl exec deployment/beeline -- beeline -u 'jdbc:hive2://hive:10000/' \
  -e 'create table if not exists test_hive_server2 (id int, name string) stored by iceberg'
kubectl exec deployment/beeline -- beeline -u 'jdbc:hive2://hive:10000/' \
  -e "insert into test_hive_server2 values (1, 'aaa'), (2, 'bbb')"
kubectl exec deployment/beeline -- beeline -u 'jdbc:hive2://hive:10000/' \
  -e 'select * from test_hive_server2'
