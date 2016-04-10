#!/bin/bash
#
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

# Add any test to be excluded in alphabetical order to keep readability, starting with files, and
# then directories.

declare -a EXCLUDE_TESTS=(
  ".*org/apache/hadoop/hive/ql/exec/.*"
  ".*org/apache/hadoop/hive/ql/parse/.*"
  ".*org/apache/hive/hcatalog/mapreduce/.*"
  ".*org/apache/hive/hcatalog/pig/.*"
)

function get_excluded_tests() {
  local IFS="|"
  echo -n "${EXCLUDE_TESTS[*]}"
}

function get_regex_excluded_tests() {
  echo -n "%regex[`get_excluded_tests`]"
}

regex_tests=`get_regex_excluded_tests`
mvn install -Dtest.excludes.additional="$regex_tests"
