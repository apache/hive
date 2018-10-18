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

THISSERVICE=fixacidkeyindex
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

fixacidkeyindex () {
  CLASS=org.apache.hadoop.hive.ql.io.orc.FixAcidKeyIndex
  HIVE_OPTS=''
  execHiveCmd $CLASS "$@"
}

fixacidkeyindex_help () {
  echo "usage ./hive --service fixacidkeyindex [-h] --check-only|--recover [--backup-path <new-path>] <path_to_orc_file_or_directory>"
  echo ""
  echo "  --check-only                Check acid orc file for valid acid key index and exit without fixing"
  echo "  --recover                   Fix the acid key index for acid orc file if it requires fixing"
  echo "  --backup-path <new_path>  Specify a backup path to store the corrupted files (default: /tmp)"
  echo "  --help (-h)                 Print help message"
}
