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

cd $(pwd)/dbs/mysql/

echo "Executing query and insert script for version : $1"

name="$(basename $1)"

if [[ "$name" = hive-schema* ]]
then
  FROM_VER=`echo $name | cut -d'-' -f3 | sed -e 's/\.mysql.sql//g'`
  SCRIPT_INSERT="mysql-$FROM_VER-insert.sql"
else
  FROM_VER=`echo $name | cut -d'-' -f2`
  TO_VER=`echo $name | cut -d'-' -f4 | sed -e 's/\.mysql.sql//g'`
  SCRIPT_INSERT="mysql-$TO_VER-insert.sql"
  SCRIPT_QUERY="mysql-$FROM_VER-query.sql"
  QUERY_RESULT="mysql-$FROM_VER-results.sql"
fi

if [[ -z "${SCRIPT_QUERY+x}" ]]
then
  echo "Nothing to query"
else
  echo "Test query script is $SCRIPT_QUERY"
  if [[ -e "$SCRIPT_QUERY" ]]
  then
    sed -i 's/"//g' $SCRIPT_QUERY
    if [[ -e "$QUERY_RESULT" ]]
    then
      rm $QUERY_RESULT
    fi
    mysql hive_hms_testing < "$(pwd)/$SCRIPT_QUERY" > "$QUERY_RESULT"

    failures=`awk '/COUNT/{nr[NR]; nr[NR+3]}; NR in nr' "$QUERY_RESULT" | grep "0" | wc -l`
    if [[ "$failures" -ne 0 ]]
    then
      echo "***** FAILED ($failures failed queries )*******"
      echo "Check $(pwd)/$QUERY_RESULT for results of the test query"
      echo `awk '/COUNT/{nr[NR]; nr[NR+3]}; NR in nr' "$QUERY_RESULT" | grep "0" | wc -l`
      exit 1
    fi

  else
    echo "$SCRIPT_QUERY does not exist, skipping ..."
  fi
fi

echo "Test insert script is $SCRIPT_INSERT"
if [[ -e "$SCRIPT_INSERT" ]]
then
  sed -i 's/"//g' $SCRIPT_INSERT

  mysql hive_hms_testing < "$(pwd)/$SCRIPT_INSERT"
else
  echo "$SCRIPT_INSERT does not exist, skipping ..."
fi
