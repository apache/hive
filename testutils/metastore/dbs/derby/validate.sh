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

# This script executes all hive metastore upgrade scripts on an specific
# database server in order to verify that upgrade scripts are working
# properly.

cd $(pwd)/dbs/derby/

echo "Executing query and insert script for version : $1"

name="$(basename $1)"

if [[ "$name" = hive-schema* ]]
then
  FROM_VER=`echo $name | cut -d'-' -f3 | sed -e 's/\.derby.sql//g'`
  SCRIPT_INSERT="derby-$FROM_VER-insert.sql"
else
  FROM_VER=`echo $name | cut -d'-' -f2`
  TO_VER=`echo $name | cut -d'-' -f4 | sed -e 's/\.derby.sql//g'`
  SCRIPT_INSERT="derby-$TO_VER-insert.sql"
  SCRIPT_QUERY="derby-$FROM_VER-query.sql"
  QUERY_RESULT="derby-$FROM_VER-results.txt"
fi

JAVA_PATH=($(readlink /etc/alternatives/java))
JAVA_PATH=`dirname $JAVA_PATH`

export JAVA_HOME=$JAVA_PATH/..
export PATH=$PATH:/usr/share/javadb/bin:$JAVA_HOME/bin

if [[ -z "${SCRIPT_QUERY+x}" ]]
then
  echo "Nothing to query"
else
  echo "Test query script is $SCRIPT_QUERY"
  if [[ -e "$SCRIPT_QUERY" ]]
  then
    echo "connect 'jdbc:derby:/tmp/hive_hms_testing;create=true';" > ./derbyQuery.sql
    echo "run '$SCRIPT_QUERY';" >> ./derbyQuery.sql
    echo "quit;" >> ./derbyQuery.sql
    ij < ./derbyQuery.sql > "$QUERY_RESULT"

    failures=`awk '/SELECT\ COUNT/{nr[NR]; nr[NR+3]}; NR in nr' "$QUERY_RESULT" | grep -B 1 "0 " | wc -l`
    if [[ "$failures" -ne 0 ]]
    then
      echo "********* FAILED ($failures failed queries)  ********"
      echo " Check $(pwd)/$QUERY_RESULT for results of test query"
      echo `awk '/SELECT\ COUNT/{nr[NR]; nr[NR+3]}; NR in nr' "$QUERY_RESULT" | grep -B 1 "0 "`
      exit 1
    fi
  else
    echo "$SCRIPT_QUERY does not exist, skipping ..."
  fi
fi

echo "Test insert script is $SCRIPT_INSERT"
if [[ -e "$SCRIPT_INSERT" ]]
then
  echo "connect 'jdbc:derby:/tmp/hive_hms_testing;create=true';" > ./derbyInsert.sql
  echo "run '$SCRIPT_INSERT';" >> ./derbyInsert.sql
  echo "quit;" >> ./derbyInsert.sql
  ij < ./derbyInsert.sql
else
  echo "$SCRIPT_INSERT does not exist, skipping ..."
fi

