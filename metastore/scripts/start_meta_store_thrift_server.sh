#!/usr/bin/env bash

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


CLASSPATH=

# the dist lib libraries
for f in /usr/local/fbprojects/hive.metastore/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the hadoop libraries
for f in /mnt/hive/stable/cluster/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# the apache libraries
for f in /mnt/hive/stable/cluster/lib/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

# for now, the fb_hive libraries
for f in /mnt/hive/stable/lib/hive/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

/usr/local/bin/java -Dcom.sun.management.jmxremote -Djava.library.path=/mnt/hive/production/cluster/lib/native/Linux-amd64-64/ -cp $CLASSPATH com.facebook.metastore.MetaStoreServer
