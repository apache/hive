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

set -e

cd $(dirname $0)

usage() {
  echo "Usage: "
  echo "  $0 --db DB_SERVER"
  echo
  echo "Options"
  echo "  --db DB_SERVER  Specifies a database server where to run the"
  echo "                  upgrade tests."
  echo
}

if [ $# != 2 ]; then
  usage && exit 1
fi

if [ $1 != "--db" ]; then
  echo "Error: unrecognized $1 option."
  echo
  usage && exit 1
fi

#
# These tests work by calling special commands from specific
# servers that are stored as scripts in a directory name
# related to the server to test.
#

OUT_LOG="/tmp/$(basename $0).log"
rm -f $OUT_LOG

log() {
	echo $@
	echo $@ >> $OUT_LOG
}

DB_SERVER="$2"
if [ ! -d "dbs/$DB_SERVER" ]; then
	log "Error: $DB_SERVER upgrade tests is not supported yet."
	echo
	echo "You must select from any of the following tests:"
  ls dbs/
  exit 1
fi

SCRIPT_PREPARE="$(pwd)/dbs/$DB_SERVER/prepare.sh"
SCRIPT_EXECUTE="$(pwd)/dbs/$DB_SERVER/execute.sh"
SCRIPT_VALIDATE="$(pwd)/metastore-validation-test.sh"
DATA_VALIDATE="$(pwd)/dbs/$DB_SERVER/validate.sh"

[ ! -e "$SCRIPT_PREPARE" ] && log "Error: $SCRIPT_PREPARE does not exist." && exit 1
[ ! -e "$SCRIPT_EXECUTE" ] && log "Error: $SCRIPT_EXECUTE does not exist." && exit 1
[ ! -e "$SCRIPT_VALIDATE" ] && log "Error: $SCRIPT_VALIDATE does not exist." && exit 1
[ ! -e "$DATA_VALIDATE" ] && log "Error: $DATA_VALIDATE does not exist." && exit 1

HMS_UPGRADE_DIR="$(pwd)/../../standalone-metastore/metastore-server/src/main/sql"
if [ ! -d "$HMS_UPGRADE_DIR/$DB_SERVER" ]; then
	log "Error: $DB_SERVER upgrade scripts are not found on $HMS_UPGRADE_DIR."
	exit 1
fi

#
# Before running any test, we need to prepare the environment.
# This is done by calling the prepare script. This script must
# install, configure and start the database server where to run
# the upgrade script.
#

log "Calling $SCRIPT_PREPARE ..."
if ! bash -e -x $SCRIPT_PREPARE; then
	log "Error: $SCRIPT_PREPARE failed. (see logs)"
	exit 1
fi
log "Server prepared."

log "Calling metastore data generation script ..."
if ! bash -e $SCRIPT_VALIDATE --db $DB_SERVER; then
        log "Error: $SCRIPT_VALIDATE failed. (see logs)"
        exit 1
fi
log "Test scripts generated."

#
# Now we execute each of the .sql scripts on the database server.
#

execute_test() {
	log "Executing sql test: $DB_SERVER/$(basename $1)"
	if ! bash -x -e $SCRIPT_EXECUTE $1; then
		log "Test failed: $DB_SERVER/$(basename $1)"
		return 1
	fi
}

VERSION_BASE="0.10.0"

HIVE_SCHEMA_BASE="$HMS_UPGRADE_DIR/$DB_SERVER/hive-schema-$VERSION_BASE.$DB_SERVER.sql"
if [ ! -f $HIVE_SCHEMA_BASE ]; then
	log "Error: $HIVE_SCHEMA_BASE is not found."
	exit 1
fi

log "Calling $SCRIPT_EXECUTE ..."

if ! execute_test $HIVE_SCHEMA_BASE; then
	echo "Error: Cannot execute SQL file: $HIVE_SCHEMA_BASE"
fi

bash -e $DATA_VALIDATE $HIVE_SCHEMA_BASE

success=$?

if [[ "$success" != 0 ]];
then
  echo "Data verification failed!!!"
  exit $success;
else
  echo "Data verification successful!!!"
fi

begin_upgrade_test="false"
while read script
do
	if [ $begin_upgrade_test = "true" ] || echo upgrade-$script | grep "upgrade-$VERSION_BASE"; then
		begin_upgrade_test="true"
		if ! execute_test $HMS_UPGRADE_DIR/$DB_SERVER/upgrade-$script.$DB_SERVER.sql; then
			echo "Error: Cannot execute SQL file: $HMS_UPGRADE_DIR/$DB_SERVER/upgrade-$script.$DB_SERVER.sql"
		fi
                bash -e $DATA_VALIDATE $HMS_UPGRADE_DIR/$DB_SERVER/upgrade-$script.$DB_SERVER.sql
                success=$?;

                if [[ $success != 0 ]];
                then
                  echo "Data verification failed!!!"
                  exit $success;
                fi
	fi
done < $HMS_UPGRADE_DIR/$DB_SERVER/upgrade.order.$DB_SERVER

log "Tests executed."
