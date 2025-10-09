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

echo "Validating testconfiguration.properties format"

HIVE_ROOT=$1
export LC_ALL=C

state="out"
row=0
last_test_name=
group=
while IFS= read -r line; do
  row=$((row+1))
  if [ "$state" == "out" ]; then
    [ -z "$line" ] && continue
    [[ $line == \#* ]] && continue

    parts=(${line//=/ })
    if [[ ${#parts[@]} != 2 ]]; then
      echo "group declaration should contain exactly one '=', but in row $row: '$line'"
      exit 1
    fi

    group=${parts[0]}
    last_test_name=
    state="in"
  else
    if ! [[ "$line" =~ [[:space:]][[:space:]]* ]]; then
      echo "lines within group should start with two spaces, but in row $row:  '$line'"
      exit 1
    fi

    file=${line:2}
    if [[ ${line: -2} == ",\\" ]]; then
      file=${file%??}
    else
      state="out"
    fi

    if ! [[ ${file: -2} == ".q" ]]; then
      echo "file name should end with '.q', but in row $row: '$line'"
      exit 1
    fi

    test_name=${file%??}
    if [[ "$test_name" = *[^a-zA-Z0-9_]* ]]; then
      echo "test name should contain only letters, numbers and '_' characters, but in row $row: '$line'"
      exit 1
    fi

    if [[ $last_test_name > $test_name ]]; then
      echo "files should be in alphabetic order within group, but in group $group in row $row: $test_name < $last_test_name "
      exit 1
    fi

    last_test_name=$test_name
  fi
done < $HIVE_ROOT/itests/src/test/resources/testconfiguration.properties

echo "Validation of testconfiguration.properties finished successfully"
