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

# This script generates data for hive metastore, inserts it into the DB
# It verifies that the data exists after the schema has been upgraded.
# Its a multi-step process.

# 1) This script parses the schema file for a database specified via --db option.
# and generates a file that contains just the "create table statements" from
# the schema.
# 2) This file is then cleansed and sorted according to the foreign
# key dependencies. The tables that have no foreign keys float above the
# ones that depend on them.
# 3) The file created in step 2 is string substituted to convert the create table
# statements into "insert into <table>" statements. During this stage, string and int
# data is generated randomly. These statements are saved to a file. This stage also
# generates another file where the select count(*) statements are generated.

# *************************************** README ************************************************
# This script is used as part of the HIVE METASTORE PRE-COMMIT build to ensure that
# any changes to the HMS schema is backward compatible with the older schema and that
# the changes do no delete any data from the current HMS datastore.
# For example, dropping a column from an existing schema table is something we want to
# avoid. Adding a column should be fine.
# This script does the following
# * Parses the hive schema file (starts at hive-0.10.0) and then compiles a list of tables
#    in this schema (into /tmp/<dbVendor>-<schemaVersion>-create.sql)
# * It then generates 1 row (for now) with random column data for each of the tables (into
#   dbs/<db>/<db>-<version>-insert.sql) for each version of the hive schema.
# * It then generates a "select count(*)" query statement for the above row so the query output
#   should be exactly ONE (1) in all cases.

# * For example:
# *   INSERT INTO TBLS ( `TBL_ID`, `CREATE_TIME`, `DB_ID`, `LAST_ACCESS_TIME`, `OWNER`, `RETENTION`,
#       `SD_ID`, `TBL_NAME`, `TBL_TYPE`, `VIEW_EXPANDED_TEXT`, `VIEW_ORIGINAL_TEXT`, `LINK_TARGET_ID` )
#       VALUES ( 1, 35142, 1, 33771, 'NOLxH4BzdiBbE8i', 83343, 88353, 'hive_testtable', 'bewmH7XTBmxb23C',
#       'JWKYyQDDYB9UsyV', 'E3AFymvLju857DD', 1 );

# *   SELECT COUNT(*) FROM TBLS where `TBL_ID` = 1 and `CREATE_TIME` = 35142 and `DB_ID` = 1 and
#       `LAST_ACCESS_TIME` = 33771 and `OWNER` = 'NOLxH4BzdiBbE8i' and `RETENTION` = 83343 and
#       `SD_ID` = 88353 and `TBL_NAME` = 'hive_testtable' and `TBL_TYPE` = 'bewmH7XTBmxb23C' and
#       `VIEW_EXPANDED_TEXT` = 'JWKYyQDDYB9UsyV' and `VIEW_ORIGINAL_TEXT` = 'E3AFymvLju857DD'  ;

# * Schema is then installed using the dbs/<db>/execute.sh.
# * The generated row data is then inserted into the DB.
# * The schema is then upgraded to the next version, says hive-schema-0.11.0. After the upgrade, this script
#   then executes the SELECT COUNT(*) queries for the tables from the prior version of schema. All tables are
#   expected to pass the test with a rowcount of 1. If not, then we know we have lost data in the process of
#   upgrade.

# * This process of upgrading schema and executing select queries continues until it has reached the latest
#   version of the schema.

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

if [ $# -lt 2 ]; then
  usage && exit 1
fi

if [ $1 != "--db" ]; then
  echo "Error: unrecognized $1 option."
  echo
  usage && exit 1
fi

DB_SERVER="$2"
if [ ! -d "dbs/$DB_SERVER" ]; then
  log "Error: $DB_SERVER upgrade tests is not supported yet."
  echo
  echo "You must select from any of the following tests:"
  ls dbs/
  exit 1
fi

#
# These tests work by calling special commands from specific
# servers that are stored as scripts in a directory name
# related to the server to test.
#

# 2 for INFO, 4 for DEBUG
LOGLEVEL=4

CONSOLELEVEL=2

# default length for int datatype
DEFAULT_INT_SIZE=5

# default length for string datatype
DEFAULT_STRING_SIZE=15

# path to the log file
OUT_LOG="/tmp/$(basename $0)-$2.log"
rm -f $OUT_LOG

getTime() {
  echo `date +"[%m-%d-%Y %H:%M:%S]"`
}

# method to print messages to the log file
log() {
  echo "$(getTime) $@" >> $OUT_LOG
}

# method to print messages to STDOUT
CONSOLE() {
  echo "$(getTime) $@"
}

# log method to print error-level messages to the log file
error() {
  if [[ "$LOGLEVEL" -ge 1 ]]
  then
    log $@
  fi

  if [[ "$CONSOLELEVEL" -ge 1 ]]
  then
    CONSOLE $@
  fi
}

# log method to print info-level messages to the log file
info() {
  if [[ "$LOGLEVEL" -ge 2 ]]
  then
    log $@
  fi

  if [[ "$CONSOLELEVEL" -ge 2 ]]
  then
    CONSOLE $@
  fi
}

# log method to print debug-level messages to the log file
debug() {
  if [[ "$LOGLEVEL" -ge 4 ]]
  then
    log $@
  fi

  if [[ "$CONSOLELEVEL" -ge 4 ]]
  then
    CONSOLE $@
  fi
}

# method that takes in 2 arguments and returns true if the second argument is a
# substring of the first.
isContains=0
contains() {
  isContains=0
  string="$1"
  substring="$2"
  if test "${string#*$substring}" != "$string"
  then
    isContains=1
  else
    isContains=0
  fi
}

# self test to ensure function is working as expected.
str1="INTEGER NOT NULL"
str2="INTEGER"
contains "$str1" "$str2"
if [[ "$isContains" -eq 1 ]]
then
  str2="VARCHAR"
  contains "$str1" "$str2"
  if [[ "$isContains" -eq 1 ]]
  then
    CONSOLE "contains self-test1 failed"
    exit 1
  fi
else
  CONSOLE "contains self-test2 failed"
  exit 1
fi


FCOUNT=0
# function to count the number of occurances of a string within another string
num_occurances() {
  fulltext="$1"
  searchChars="$2"
  FCOUNT=0
  debug "[num_occurances] Search params $fulltext:$searchChars"
  FCOUNT=$(grep -o "$searchChars" <<< "$fulltext" | wc -l)
  debug "[num_occurances] Returning $FCOUNT"
}

# self test to ensure function is working as expected.
str1="( INTEGER ())) NOT NULL )"
num_occurances "$str1" "("
if [[ $FCOUNT -eq 2 ]]
then
  num_occurances "$str1" ")"
  if [[ $FCOUNT -ne 4 ]]
  then
    CONSOLE "num_occurances self-test failed"
    exit 1
  fi
else
  CONSOLE "num_occurances self-test failed"
  exit 1
fi

# array that store all legal datatypes from all DB vendors
TYPES=( "bigint"
        "smallint"
        "int"
        "integer"
        "tinyint"
        "boolean"
        "long varchar"
        "varchar"
        "char"
        "character varying"
        "character"
        "bytea"
        "binary"
        "varbinary"
        "mediumtext"
        "double"
        "double precision"
        "serial"
        "bigserial"
        "bit"
        "BIGINT"
        "SMALLINT"
        "LONG VARCHAR"
        "VARCHAR"
        "INT"
        "INTEGER"
        "TINYINT"
        "BOOLEAN"
        "CHAR"
        "CHARACTER VARYING"
        "CHARACTER"
        "BYTEA"
        "BINARY"
        "VARBINARY"
        "MEDIUMTEXT"
        "DOUBLE"
        "DOUBLE PRECISION"
        "SERIAL"
        "BIGSERIAL"
        "BIT"
        )

# function to create a new map-type datastructure.
# this is implemented as file on the local filesystem that contains name/value pairs.
# first argument is the name of the map. filename matches the map name.
newhash() {
  rm -f /tmp/hashmap.$1
  touch /tmp/hashmap.$1
}

# function to add an entry to the map. Entry is always added to the end of the map.
# so if you delete and re-add an entry to the map, its position within a file could change.
# arg1 is name of the map to add to.
# arg2 is the key for entry.
# arg3 is the value for the map entry.
put() {
  echo "$2=$3" >> /tmp/hashmap.$1
}

# function to retrieve an entry from the map.
# arg1 is name of the map to retrive from.
# arg2 is the key for entry
get() {
  grep "^$2=" /tmp/hashmap.$1 | cut -d'=' -f2
}

# function that returns the size of the map (aka number if entries)
# arg1 is name of the map to retrive from.
size() {
  echo `cat /tmp/hashmap.$1 | wc -l`
}

# function that returns the key for the first entry in the map.
# maps to the key of the first line in the file on disk.
# arg1 is name of the map to retrive from.
firstKey() {
  head -n 1 /tmp/hashmap.$1 | cut -d'=' -f1
}

# function that returns the value of the first entry in the map.
# maps to the value of the first line in the file on disk.
# arg1 is name of the map to retrive from.
firstValue() {
  head -n 1 /tmp/hashmap.$1 | cut -d'=' -f2
}

# function that returns true if the map contains the specified key
# arg1 is name of the map to retrieve from.
# arg2 key to be found in the map.
containsKey() {
  # find a line that contains the key value at the begining of line.
  ret=$(grep "^$2=" /tmp/hashmap.$1 | cut -d'=' -f1)
  if [[ "$2" = "$ret" ]]
  then
    echo "true"
  else
   echo "false"
  fi
}

# function that is specific to a map named "f_keys"
# This map contains foreign key references. Given a table name, this function
# returns the a COMMA-separated list of tables its keys references
# arg1 is name of the map to retrieve from.
# arg2 name of the table to return references for.
get_referenced_tables() {
  if [[ ! "$1" = "f_keys" ]]
  then
    echo "NONE"
    return
  fi

  tables=""
  while read fkc
  do
    key=`echo $fkc | cut -d'=' -f1`
    table=`echo $key | cut -d'.' -f1`
    if [[ "$table" != "$2" ]]
    then
      continue
    fi

    value=`echo $fkc | cut -d'=' -f2`
    reftable=`echo $value | cut -d'.' -f1`
    if [[ "$reftable" = "$table" ]]
    then
      continue
    fi

    if [[ "$tables" = "" ]]
    then
      tables="$reftable"
    else
      tables="$tables,$reftable"
    fi

  done < /tmp/hashmap.$1;
  echo "$tables"
}

# function that deletes an entry from a named map.
# arg1 is name of the map to delete from.
# arg2 key of the entry to delete from the map.
delete() {
  if [[ "$1" = "" ]]
  then
    return
  fi
  debug "[delete] deleting entry for $2"
  todo=$(size "$1")
  exists=`cat /tmp/hashmap.$1 | grep "^$2=" | wc -l`
  debug "[delete] entry found in map=$exists"
  if [[ "$exists" -ge "1" ]]
  then
    if [[ "$todo" == 1 ]]
    then
      rm /tmp/hashmap.$1
      touch /tmp/hashmap.$1
      return
    fi
  fi
  cat /tmp/hashmap.$1 | grep -v "^$2=" > /tmp/hashmap.$1.tmp
  mv /tmp/hashmap.$1.tmp /tmp/hashmap.$1
  todo=$(size "unsorted")
  debug "[delete] map size after delete:$todo"
}

is_key_like() {
  if [[ "$1" = "" ]]
  then
    return 0
  fi

  matched=0
  while read fkc
  do
    local tabvalue=$(echo $fkc | grep "^$2." | cut -d'=' -f1 | cut -d'.' -f1)
    if [[ "$2" = "$tabvalue" ]]
    then
      matched=1
      break
    fi
  done < /tmp/hashmap.$1;
  echo "[is_key_like] found match for $2:$matched"
}

# a map that maps SQL datatypes to datatype for the values to be generated.
newhash typemap
put typemap "bigint" "int"
put typemap "smallint" "smallint"
put typemap "int" "int"
put typemap "integer" "int"
put typemap "tinyint" "tinyint"
put typemap "bit" "bit"
put typemap "boolean" "boolean"
put typemap "mediumint" "int"
put typemap "long varchar" "longstring"
put typemap "varchar" "string"
put typemap "char" "string"
put typemap "character" "string"
put typemap "bytea" "string"
put typemap "binary" "string"
put typemap "varbinary" "string"
put typemap "character varying" "string"
put typemap "mediumtext" "string"
put typemap "double" "int"
put typemap "double precision" "int"
put typemap "serial" "int"
put typemap "bigserial" "int"
put typemap "BIGINT" "int"
put typemap "SMALLINT" "smallint"
put typemap "MEDIUMINT" "int"
put typemap "INT" "int"
put typemap "INTEGER" "int"
put typemap "TINYINT" "tinyint"
put typemap "BIT" "bit"
put typemap "BOOLEAN" "boolean"
put typemap "LONG VARCHAR" "longstring"
put typemap "VARCHAR" "string"
put typemap "CHAR" "string"
put typemap "BYTEA" "string"
put typemap "BINARY" "string"
put typemap "VARBINARY" "string"
put typemap "CHARACTER" "string"
put typemap "CHARACTER VARYING" "string"
put typemap "MEDIUMTEXT" "string"
put typemap "DOUBLE" "int"
put typemap "DOUBLE PRECISION" "int"
put typemap "SERIAL" "int"
put typemap "BIGSERIAL" "int"

# a map that contains mappings of foreign key references within the tables.
# this is sort of hard to parse out of SQL. At some point in the future I will
# look to have this map auto-generated.
newhash "f_keys"
put "f_keys" "BUCKETING_COLS.SD_ID" "SDS.SD_ID"
put "f_keys" "COLUMNS.SD_ID" "SDS.SD_ID"
put "f_keys" "COLUMNS_OLD.SD_ID" "SDS.SD_ID"
put "f_keys" "COLUMNS_V2.CD_ID" "CDS.CD_ID"
put "f_keys" "DATABASE_PARAMS.DB_ID" "DBS.DB_ID"
put "f_keys" "DB_PRIVS.DB_ID" "DBS.DB_ID"
put "f_keys" "FUNCS.DB_ID" "DBS.DB_ID"
put "f_keys" "FUNC_RU.FUNC_ID" "FUNCS.FUNC_ID"
put "f_keys" "IDXS.INDEX_TBL_ID" "TBLS.TBL_ID"
put "f_keys" "IDXS.ORIG_TBL_ID" "TBLS.TBL_ID"
put "f_keys" "IDXS.SD_ID" "SDS.SD_ID"
put "f_keys" "INDEX_PARAMS.INDEX_ID" "IDXS.INDEX_ID"
put "f_keys" "PARTITIONS.LINK_TARGET_ID" "PARTITIONS.PART_ID"
put "f_keys" "PARTITIONS.SD_ID" "SDS.SD_ID"
put "f_keys" "PARTITIONS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "PARTITION_KEYS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "PARTITION_KEY_VALS.PART_ID" "PARTITIONS.PART_ID"
put "f_keys" "PARTITION_PARAMS.PART_ID" "PARTITIONS.PART_ID"
put "f_keys" "PART_COL_PRIVS.PART_ID" "PARTITIONS.PART_ID"
put "f_keys" "PART_COL_STATS.PART_ID" "PARTITIONS.PART_ID"
put "f_keys" "PART_PRIVS.PART_ID" "PARTITIONS.PART_ID"
put "f_keys" "ROLE_MAP.ROLE_ID" "ROLES.ROLE_ID"
put "f_keys" "SDS.CD_ID" "CDS.CD_ID"
put "f_keys" "SDS.SERDE_ID" "SERDES.SERDE_ID"
put "f_keys" "SD_PARAMS.SD_ID" "SDS.SD_ID"
put "f_keys" "SERDE_PARAMS.SERDE_ID" "SERDES.SERDE_ID"
put "f_keys" "SKEWED_COL_NAMES.SD_ID" "SDS.SD_ID"
put "f_keys" "SKEWED_COL_VALUE_LOC_MAP.SD_ID" "SDS.SD_ID"
put "f_keys" "SKEWED_COL_VALUE_LOC_MAP.STRING_LIST_ID_KID" "SKEWED_STRING_LIST.STRING_LIST_ID"
put "f_keys" "SKEWED_STRING_LIST_VALUES.STRING_LIST_ID" "SKEWED_STRING_LIST.STRING_LIST_ID"
put "f_keys" "SKEWED_VALUES.SD_ID_OID" "SDS.SD_ID"
put "f_keys" "SKEWED_VALUES.STRING_LIST_ID_EID" "SKEWED_STRING_LIST.STRING_LIST_ID"
put "f_keys" "SORT_COLS.SD_ID" "SDS.SD_ID"
put "f_keys" "TABLE_PARAMS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "TAB_COL_STATS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "TBLS.DB_ID" "DBS.DB_ID"
put "f_keys" "TBLS.LINK_TARGET_ID" "TBLS.TBL_ID"
put "f_keys" "TBLS.SD_ID" "SDS.SD_ID"
put "f_keys" "TBL_COL_PRIVS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "TBL_PRIVS.TBL_ID" "TBLS.TBL_ID"
put "f_keys" "TXN_COMPONENTS.TC_TXNID" "TXNS.TXN_ID"
put "f_keys" "TYPE_FIELDS.TYPE_NAME" "TYPES.TYPES_ID"

# store generated values in map and print it to a file
# resetting the values from any prior runs
newhash "genvalues"

# store names of the tables that have already been sorted
# resetting the values from any prior runs
newhash "sorted"

skipParsing=0
skipGeneration=0
if [ "$3" = "--skipParsing" ]; then
  debug "skipParsing is true"
  skipParsing=1
fi

if [ "$3" = "--skipProcessing" ]; then
  debug "skipProcessing is true"
  skipParsing=1
  skipGeneration=1
fi

VERSION_BASE="0.10.0"
VERSION_CURR="0.10.0"
SQL_OUT="/tmp/$DB_SERVER-$VERSION_CURR-create.sql"
SQL_SORTED="/tmp/$DB_SERVER-$VERSION_CURR-sortedcreate.sql"
SCRIPT_INSERT="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-$VERSION_CURR-insert.sql"
SCRIPT_QUERY="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-$VERSION_CURR-query.sql"
INSERTALL="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-insertall.sql"
QUERYALL="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-queryall.sql"

printSQL() {
  echo $@ >> $SQL_OUT
}

# method to delete a file from the file system.
rmfs() {
  if [[ -e "$1" ]]
  then
    debug "Deleting generated file $1"
    rm "$1"
  fi
}

DATAFILE="$(pwd)/dataload.properties"

# method that returns a static value loaded from the properties file for a column. These values are read from a
# file on disk
getStaticValue() {
  grep -i "^$1=" "$DATAFILE" | cut -d'=' -f2
}

HMS_UPGRADE_DIR="$(pwd)/../../metastore/scripts/upgrade"
if [ ! -d "$HMS_UPGRADE_DIR/$DB_SERVER" ]; then
  error "Error: $DB_SERVER upgrade scripts are not found on $HMS_UPGRADE_DIR."
  exit 1
fi

HIVE_SCHEMA_BASE="$HMS_UPGRADE_DIR/$DB_SERVER/hive-schema-$VERSION_BASE.$DB_SERVER.sql"

SCRIPT_SRC="$HIVE_SCHEMA_BASE"

if [ "$skipParsing" -eq "0" ]; then
  rmfs "$SQL_OUT"
  rmfs "$SQL_SORTED"
  rmfs "$SCRIPT_INSERT"
  rmfs "$SCRIPT_QUERY"
  rmfs "$INSERTALL"
  rmfs "$QUERYALL"
else
  info "skipParsing flag enabled, skipping parsing for $SCRIPT_SRC"
fi

if [ "$skipGeneration" -eq "0" ] && [ "$skipParsing" -eq "1" ]; then
  rmfs "$SCRIPT_INSERT"
  rmfs "$SCRIPT_QUERY"
  rmfs "$INSERTALL"
  rmfs "$QUERYALL"
fi

#
# Before running any test, we need to prepare the environment.
# This is done by calling the prepare script. This script must
# install, configure and start the database server where to run
# the upgrade script.
#

execute_test() {
  log "Executing sql test: $DB_SERVER/$(basename $1)"
  if ! bash -x -e $SCRIPT_EXECUTE $1; then
    log "Test failed: $DB_SERVER/$(basename $1)"
    return 1
  fi
}

# this method parses the SQL schema file and generates a file with all the
# "create table" definitions. This file is later string-subbed to generate
# an file with bunch of "insert into <table>" statements.
parse_schema() {
  if [ -e $SQL_OUT ] && [ "$2" = 0 ]
  then
    debug "[parse_schema] Resetting position in file"
    rmfs "$SQL_OUT"
  fi

  info "[parse_schema] Parsing sql file: $1"
  CREATE_OPEN=false
  TABLE_DEF=""
  COUNTER=0
  shopt -s nocasematch

  while read line
  do
    copy=$line
    # if the line is a comment, begins with #, skip it
    if [[ "$copy" =~ "^--" ]]
    then
      debug "[parse_schema] Comment, skipping ..."
      continue
    fi

    # The following code counts the number of open an close parenthesis within
    # each line read from the schema file. When the openparen count matches the
    # closeparen count, it assumes that the create table definition has ended.
    debug "[parse_schema] Processing $line with $COUNTER and $CREATE_OPEN"
    if [[ "$CREATE_OPEN" = true ]]
    then
      ignore=$(is_throwaway "$copy")
      debug "ignore=$ignore=line=$copy="
      if [[ "$ignore" = "TRUE" ]]
      then
        debug "ignoring line"
        continue
      fi

      #if [[ "$copy" =~ "(" ]]
      num_occurances "$copy" "("
      if [[ $FCOUNT -gt 0 ]]
      then
        COUNTER=$((COUNTER+FCOUNT))
        debug "[parse_schema] count=$COUNTER"
      fi

      #if [[ "$copy" =~ ")" ]]
      num_occurances "$copy" ")"
      if [[ $FCOUNT -gt 0 ]]
      then
        if [[ "$COUNTER" -gt 1 ]]
        then
          debug "[parse_schema] Adding line to table: $line :: $COUNTER"
          TABLE_DEF="$TABLE_DEF $line"
          debug "[parse_schema] TABLEDEF=$TABLE_DEF"
          COUNTER=$((COUNTER-FCOUNT))
          debug "[parse_schema] count=$COUNTER"
        elif [[ "$COUNTER" -eq 1 ]]
        then
          copy=$line
          debug "[parse_schema] CLOSE FOUND $line"
          PART=`echo $copy | cut -d')' -f1 | sed 's/$/)/'`
          debug "[parse_schema] $PART appended to table def"
          TABLE_DEF="$TABLE_DEF $PART"
          COUNTER=0
          CREATE_OPEN=false
          debug "[parse_schema] Full Table definition is : $TABLE_DEF"
          printSQL "$TABLE_DEF"
        fi
      else
        debug "[parse_schema] Adding line to table: $line :: $COUNTER"
        TABLE_DEF="$TABLE_DEF $line"
        debug "[parse_schema] TABLEDEF=$TABLE_DEF"
      fi
    else
      if [[ "$copy" =~ "create table" ]];
      then
        debug "[parse_schema] OPEN FOUND $line"
        TABLE_DEF="$line"

        num_occurances "$copy" ")"
        local closeCount=$FCOUNT
        num_occurances "$copy" "("
        local openCount=$FCOUNT
        debug "[parse_schema] matches $openCount $closeCount"
        if [ "$openCount" -eq "$closeCount" ] && [ "$openCount" -gt 0 ]
        then
          PART=`echo $copy | cut -d';' -f1 | sed 's/$/;/'`
          printSQL "$PART"
          COUNTER=0
          CLOSE_OPEN=false
          continue
        fi

        CREATE_OPEN=true
        if [[ "$FCOUNT" -gt 0 ]]
        then
          COUNTER=$((COUNTER+FCOUNT))
          debug "[parse_schema] count=$COUNTER"
        fi
      fi
    fi
  done < "$1";
}

# function to detemine if an argument is a number. This is used to determine the
# length of the column values that are to be generated.
is_int() {
  return $(test "$@" -eq "$@" > /dev/null 2>&1);
}

# self-test to ensure function is working as expected
if $(is_int "500");
then
  if $(is_int "foo");
  then
    # function not working properly
    exit 1
  fi
else
  # function not working properly
  exit 1
fi

# trims the leading and trailing spaces from a line
trim() {
  local var="$*"
  var="${var#"${var%%[![:space:]]*}"}"   # remove leading whitespace characters
  var="${var%"${var##*[![:space:]]}"}"   # remove trailing whitespace characters
  echo -n "$var"
}

# function that generates a random int value of default size
int() {
  limit="$DEFAULT_INT_SIZE"
  if $(is_int "${1}");
  then
    if [[ "$1" -lt "$limit" ]]
    then
      limit="$1"
    fi
  fi

  limit_int "$limit"
}

# function that generates a random int value of a specified length.
# if the limit passed in is NOT a number, an int of default length is returned.
limit_int() {
  if $(is_int "${1}");
  then
    limit="$1"
  else
    limit=5
  fi

  if [ "$(uname)" == "Darwin" ]; then
    cat /dev/urandom | env LC_CTYPE=C tr -dc '1-9' | fold -w ${limit:-15} | head -n 1
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    cat /dev/urandom | tr -dc '1-9' | fold -w ${limit:-15} | head -n 1
  fi
  #cat /dev/urandom | env LC_CTYPE=C tr -dc '1-9' | fold -w ${1:-15} | head -n 1
}

# function that returns a random bit value of either 0 or 1
bit() {
  r_int=$(int)
  echo $[ $[r_int % 2 ]]
}

# function that returns a random boolean value of TRUE or FALSE
boolean() {
  trueval="TRUE"
  falseval="FALSE"

  # have to special case derby because derby has no native bit datatype
  # it uses a char of length 1 to store Y or N for booleans.
  if [[ "$DB_SERVER" = "derby" ]]
  then
    trueval="Y"
    falseval="N"
  fi

  r_bit=$(bit)
  if [[ "$r_bit" == 1 ]]
  then
    echo "$trueval"
  else
    echo "$falseval"
  fi
}

tinyint() {
  r_int=$(int)
  echo $[ $[r_int % 127 ]]
}

smallint() {
  r_int=$(int)
  echo $[ $[r_int % 32000 ]]
}

hex() {
  input="$1"
  hexval=$(xxd -pu <<< "$input")
  echo "$hexval"
}

limit_string() {
  if $(is_int "${1}");
  then
    limit="$1"
  else
    limit=5
  fi

  # for derby a char of length generates either a Y or N
  # serves well for boolean types in the schema
  if [[ ( "$DB_SERVER" = "derby" ) && ( "$limit" -eq 1 ) ]]
  then
    boolean
    return
  fi

  if [ "$(uname)" == "Darwin" ]; then
    cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | fold -w ${limit:-32} | head -n 1
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    cat /dev/urandom | tr -dc 'a-z0-9A-Z' | fold -w ${limit:-32} | head -n 1
  fi
}

# function that returns a random string of default length.
string()
{
  limit="$DEFAULT_STRING_SIZE"
  if $(is_int "${1}");
  then
    if [[ "$1" -lt "$limit" ]]
    then
      limit="$1"
    fi
  fi

  limit_string "$limit"
}

# function to determine if a token within a column definition is to be ignored.
# this is to differentiate column definitions from PRIMARY/FOREIGN KEY specifications.
is_throwaway() {
  coldef=$(trim "$1")
  if [[ ( "$coldef" =~ ^PRIMARY\ KEY.* ) || ( "$coldef" =~ FOREIGN\ KEY ) ]]
  then
    echo "TRUE"
  else
    echo "FALSE"
  fi
}

# extracts the name of the columns from a column definition within SQL.
# Assumes that the first token is the name of the column
extract_colname() {
  coldef=$(trim "$1")
  echo $coldef | cut -d' ' -f1
}

# returns properly quoted column string, with ticks, single or double quotes according to the DB's preference.
# Assumes that the first token is the name of the column
localize_colname() {
  coldef=$(trim "$1")
  if [[ "$DB_SERVER" = "mysql" ]]
  then
    echo "\`$coldef\`"
  elif [[ "$DB_SERVER" = "derby" ]]
  then
    echo "\"$coldef\""
  else
    echo "$coldef"
  fi
}

# extracts the name of the table from a create statement from the parsed SQL.
# Assumes that the thrid SPACE-separated token is the name of the table.
# ex: CREATE TABLE DBS ( DB_ID bigint(20) );
extract_tablename() {
  if [[ "$DB_SERVER" = "derby" ]]
  then
    stmt=`echo "$1" | cut -d' ' -f3 | cut -d'(' -f1 | sed -e 's/APP.//g'`
  else
    #stmt=`echo "$1" | cut -d' ' -f3`
    stmt=`echo "$1" | cut -d'(' -f1 | cut -d' ' -f3`
  fi
  echo "$stmt" | tr -d '"'
}

# this function inserts the comma-separated column names for the table
# into the insert statement
# first argument is the current SQL insert statement, the second is a list of columns for this table
# ex: INSERT INTO foo VALUES (1, "ABC")
# becomes
# INSERT INTO foo (id, name) VALUES (1, "ABC")
insert_column_names() {
  line="$1"
  colnames="$2"
  merged=`echo "$line" | sed -e "s/\ VALUES\ /\ ${colnames}\ VALUES\ /g"`
  echo "$merged"
}

# extracts the name of the table in its native form from a create statement from the parsed SQL.
# Assumes that the thrid SPACE-separated token is the name of the table. if there are quotes around it will 
# return the quotes too.
# ex: CREATE TABLE DBS ( DB_ID bigint(20) );
extract_raw_tablename() {
  if [[ "$DB_SERVER" = "derby" ]]
  then
    stmt=`echo "$1" | cut -d' ' -f3 | cut -d'(' -f1 | sed -e 's/APP.//g'`
  else
    stmt=`echo "$1" | cut -d'(' -f1 | cut -d' ' -f3`
  fi
  echo "$stmt"
}

# function to extract the maxlength of the column from the column definition.
# if the column definition contains (), then the value within the () is the length.
# 100 otherwise.
extract_columnsize() {
  if test "${1#*(}" != "$1"
  then
    echo "$1" | cut -d "(" -f2 | cut -d ")" -f1
  else
    echo "100"
  fi
}

# this functions reads all the create table statements from the file generated by
# parse_schema and then sorts them according to the order in which the insert
# statements are to be generated. It uses the foreign key references to sort the tables.
# Any tables without FK references appear above the tables with FKs to these.
# if table B's column has a FK reference to table A's column, this sort function
# guarantees that table A appears before table B in the sorted file.
sort_tabledefs() {
  if [[ -e $SQL_SORTED ]] && [[ "$1" = 0 ]]
  then
    debug "[sort] Resetting the position in file"
    rmfs "$SQL_SORTED"
    #return
  fi

  if [ ! -e $SQL_OUT ]
  then
    info "[sort] $SQL_OUT does not exist, returning .."
    return
  fi

  info "[sort] Sorting table definitions based on dependencies"
  newhash "unsorted"

  while read line
  do
    table=$(extract_tablename "$line")
    debug "[sort] Extracted table name=$table from $line"
    put "unsorted" "$table" "$line"
  done < $SQL_OUT;

  table=""
  todo=$(size "unsorted")
  debug "[sort] $todo entries are to be sorted"
  while [[ "$todo" > 0 ]]
  do
    table=$(firstKey "unsorted")
    line=$(firstValue "unsorted")
    debug "[sort] table name $table"
    references=$(get_referenced_tables "f_keys" "$table")
    debug "[sort] table $table references $references"
    if [[ ! "$references" = "" ]]
    then
      commit="true"
      OLDIFS=$IFS
      IFS=',' eval 'array=($references)'
      IFS=$OLDIFS
      for i in "${!array[@]}"
      do
        hashsize=$(size "sorted")
        if [[ "$hashsize" > 0 ]]
        then
          hasKey=$(containsKey "sorted" "${array[i]}")
          debug "[sort] is table ${array[i]} sorted:$hasKey"
          if [[ ! "$hasKey" = true ]]
          then
            commit="false"
            break
          fi
        else
          commit="false"
        fi
      done
      debug "[sort] commit table $table:$commit"
      if [[ "$commit" = "true" ]]
      then
        echo "$line" >> $SQL_SORTED
        put "sorted" "$table" "$line"
        debug "[sort] deleting table $table from unsorted"
        delete "unsorted" "$table"
        debug "[sort] table $table committed"
      else
        delete "unsorted" "$table"
        put "unsorted" "$table" "$line"
        debug "[sort] table $table deferred"
      fi
    else
      echo "$line" >> $SQL_SORTED
      put "sorted" "$table" "$line"
      delete "unsorted" "$table"
      debug "[sort] table $table committed as it has no references"
    fi

    todo=$(size "unsorted")
    debug "[sort] todo size=$todo"
  done
  info "[sort] Sorting Complete!!!"
}

# this functions performs some cleansing on the SQL statements to a unified format.
# for example, mysql schema file contains "create table if not exists" while PG and derby
# just have "create table"
cleanse_sql() {
  info "[Cleansing] Cleansing data"
  TMP="/tmp/insert-$DB_SERVER-$VERSION_CURR-tmp.sql"
  TMP2="/tmp/insert-$DB_SERVER-$VERSION_CURR-tmp2.sql"
  if [[ -e "$SQL_OUT" ]]
  then
    if [[ "$DB_SERVER" = "postgres" ]]
    then
      # TODO perhaps there is a better means to do this with just cat and set
      # but due to inconsistencies in the schema files for postgres, some tables are upper case and some tables are lowercase
      # CREATE TABLE TXNS --> creates a lower case "txns" table
      # CREATE TABLE "TXNS" --> creates a upper case "TXNS" table
      # so we have retain the quotes or lack thereof from the original create table command.
      while read line; do
        # so we cut the statement into 2 parts and only massage the second part.
        part1="$( cut -d '(' -f 1 <<< "$line" )"
        part1modified=$(echo $part1 | sed -e 's/IF\ NOT\ EXISTS\ //g')
        part2="$( cut -d '(' -f 2- <<< "$line" )"
        echo "$part1modified( $part2" >> $TMP
      done < $SQL_OUT
    else
      cat $SQL_OUT | sed -e 's/"//g' | sed -e 's/`//g' | sed -e 's/IF\ NOT\ EXISTS\ //g' > $TMP
    fi

    if [[ "$DB_SERVER" = "mysql" ]]
    then
      cat $TMP | tr -s ' ' | sed 's/,\ PRIMARY\ KEY.*$/)/g' > $TMP2
      mv $TMP2 $TMP
    fi
    mv $TMP $SQL_OUT
  fi
  info "[Cleansing] Cleansing complete!!"
}

# this is third and final step in the converting the file with "create table" statements
# into a file with "insert into" statements where the column values are either looked from
# from the static data file or generated according to their datatype determined in step 1
generate_data() {
  info "[generate_data] SQL inserts will be saved to $SCRIPT_INSERT, SQL query to $SCRIPT_QUERY"
  if [ -e $SCRIPT_INSERT ] && [ -e $SCRIPT_QUERY ] && [ "$1" = 0 ]
  then
    debug "[sort] Skipping data generation"
    rmfs "$SCRIPT_INSERT"
    rmfs "$SCRIPT_QUERY"
  fi

  if [[ -e "$QUERYALL" ]]
  then
    cp "$QUERYALL" "$SCRIPT_QUERY"
  fi

  if [[ ! -e $SQL_SORTED ]]
  then
    info "[generate_data] $SQL_SORTED does not exist, returning .."
    return
  fi

  info "[generate_data] Generating table data for $DB_SERVER-$VERSION_CURR"
  INSERT="/tmp/$DB_SERVER-$VERSION_CURR-insert.sql"
  TMP="/tmp/insert-$DB_SERVER-$VERSION_CURR-tmp.sql"
  cat $SQL_SORTED | sed -e 's/IF\ NOT\ EXISTS\ //g' | sed -e 's/CREATE\ TABLE/INSERT\ INTO/g' | sed -e 's/(/\ VALUES\ \(/' > $INSERT

  while read line
  do
    debug "[generate_data] Processing...$line"
    BEGIN=`echo $line | cut -d'(' -f1 | sed 's/$/(/'`
    insert_cols="("
    select="SELECT COUNT(*) FROM"
    REST=`echo $line | cut -d'(' -f2-`
    table=$(extract_raw_tablename "$BEGIN")
    select="$select $table where"
    table=`echo $table | tr -d '"'`
    OLDIFS=$IFS
    IFS=',' eval 'array=($REST)'
    IFS=$OLDIFS
    hasOpen=0
    hasClose=0
    tokenbuffer=""
    for i in "${!array[@]}"
    do
      token="${array[i]}"
      debug "[generate_data] Processing table:$table, token=>${array[i]},hasOpen=$hasOpen,hasClose=$hasClose"

      debug "token value before extracting column=$token"
      colname=$(extract_colname "$token")
      unquotedColname=$(echo $colname | sed -e 's/"//g')
      sqlColname=$(localize_colname "$colname")

      debug "[generate_data] Processing table:$table, column:$colname, tablename.columnname is $table.$colname"

      # the value for this column is generated by the DB. The column def for such DB managed
      # columns is very different. A value of default has to be supplied for this key.
      if [[ "$table.$colname" == "MASTER_KEYS.KEY_ID" ]]
      then
        info "$table.$colname is generated value"
        BEGIN="$BEGIN DEFAULT,"
        insert_cols="$insert_cols $sqlColname,"
        continue
      fi

      for type in "${!TYPES[@]}"
      do
        fk=""
        val=""
        # TODO we are over-iterating here for each column
        debug "[generate_data] column name=$colname"
        staticValue=$(getStaticValue "$table.$colname")
        if [[ ! "$staticValue" = "" ]]
        then
          info "pre-loaded value for $table.$colname is $staticValue"
          put "genvalues" "$table.$colname" "$staticValue"
          BEGIN="$BEGIN $staticValue,"
          insert_cols="$insert_cols $sqlColname,"
          if [ ! "$table.$colname" = "TBLS.LINK_TARGET_ID" ] && [ ! "$table.$colname" = "PARTITIONS.LINK_TARGET_ID" ]
          then
            select="$select $sqlColname = $staticValue and"
          fi
          continue 2
        fi

        contains "$token" "${TYPES[type]}"
        if [[ $isContains -eq 1 ]]
        then
          mappedtype=$(get "typemap" "${TYPES[type]}")
          if [[ "$DB_SERVER" -eq "derby" ]]
          then
            contains "$token" "for bit data"
            if [[ $isContains -eq 1 ]]
            then
              mappedtype="derbybinary"
            fi
          fi

          debug "[generate_data] column type for $table.$colname is ${TYPES[type]}, mapped type=$mappedtype"
          colsize=$(extract_columnsize "$token")
          debug "[generate_data] columnsize=$colsize for $table.$colname"
          fk=$(get "f_keys" "$table.$unquotedColname")
          if [[ ! "$fk" = "" ]]
          then
            val=$(get "genvalues" "$fk")
            info "[generate_data] column $table.$unquotedColname references $fk whose value is $val"
          fi

          if [[ "$val" != "" ]]
          then
            BEGIN="$BEGIN $val,"
            insert_cols="$insert_cols $sqlColname,"
            if [ ! "$table.$colname" = "TBLS.LINK_TARGET_ID" ] && [ ! "$table.$colname" = "PARTITIONS.LINK_TARGET_ID" ]
            then
              select="$select $sqlColname = $val and"
            fi
            continue 2
          fi

          info "[generate_data] Generating random value for column $table.$unquotedColname"
          case "$mappedtype" in
            int )
              if [[ "$val" = "" ]]
              then
                val=$(int "$colsize")
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
            longstring )
              if [[ "$val" = "" ]]
              then
                val=$(string "$colsize")
                val="'$val'"
                put "genvalues" "$table.$unquotedColname" "$val"
              fi

              BEGIN="$BEGIN $val,"
              insert_cols="$insert_cols $sqlColname,"
              select="$select TRIM($sqlColname) = TRIM($val) and"
              continue 2
              ;;
            string )
              if [[ "$val" = "" ]]
              then
                val=$(string "$colsize")
                val="'$val'"
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
            bit )
              if [[ "$val" = "" ]]
              then
                val=$(bit)
                put "genvalues" "$table.$unquotedColname" "$val"
              fi

              BEGIN="$BEGIN $val,"
              insert_cols="$insert_cols $sqlColname,"
              select="$select CAST( $sqlColname AS UNSIGNED) = '$val' and"
              continue 2
              ;;
            boolean )
              if [[ "$val" = "" ]]
              then
                val=$(boolean)
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
            tinyint )
              if [[ "$val" = "" ]]
              then
                val=$(tinyint)
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
            smallint )
              if [[ "$val" = "" ]]
              then
                val=$(smallint)
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
            derbybinary )
              if [[ "$val" = "" ]]
              then
                val=$(string "$colsize")
                val=$(hex "$val")
                val="X'$val'"
                put "genvalues" "$table.$unquotedColname" "$val"
              fi
              ;;
          esac

          BEGIN="$BEGIN $val,"
          insert_cols="$insert_cols $sqlColname,"
          if [ ! "$table.$unquotedColname" = "TBLS.LINK_TARGET_ID" ] && [ ! "$table.$unquotedColname" = "PARTITIONS.LINK_TARGET_ID" ]
          then
            select="$select $sqlColname = $val and"
          fi
          break
        fi
      done
    done
    BEGIN="${BEGIN: 0:${#BEGIN} - 1} );"
    insert_cols="${insert_cols: 0:${#insert_cols} - 1} )"
    foo=$(insert_column_names "$BEGIN" "$insert_cols")
    select="${select: 0:${#select} - 3} ;"
    echo "$foo" >> $SCRIPT_INSERT
    echo "$select" >> $SCRIPT_QUERY
    echo "$select" >> $QUERYALL
  done < $INSERT;
}

if [ ! -f $HIVE_SCHEMA_BASE ]; then
  log "Error: $HIVE_SCHEMA_BASE is not found."
  exit 1
fi

processFile() {

  debug "[processFile] $VERSION_CURR"
  debug "parsed sql output file is $SQL_OUT, sorted file is $SQL_SORTED"
  debug "generated sql script is $SCRIPT_INSERT, query file is $SCRIPT_QUERY"

  if [ -e "$SQL_OUT" ] && [ -e "$SQL_SORTED" ] && [ "$skipParsing" -eq "1" ];
  then
    info "skipParsing flag enabled, skipping parsing for $HIVE_SCHEMA_BASE"
  else
    debug "Invoking the parsing routine for [$1]"
    parse_schema "$1" "$2"
    debug "Invoking the cleansing routine for[$1]"
    cleanse_sql
    debug "Sorting tables for [$1]"
    sort_tabledefs "$2"
  fi

  if [ -e "$SQL_SORTED" ] && [ "$skipGeneration" -eq "0" ];
  then
    debug "Invoking the generate routine for[$1]"
    generate_data "$2"
  fi

  debug "[$1] DONE!!"
}

parseAndAppend()
{
  info "[parseAndAppend] $SQL_OUT:$SCRIPT_INSERT:$SQL_SORTED for $VERSION_CURR"
  if [ -e $SQL_OUT ] && [ -e $SQL_SORTED ] && [ "$skipParsing" -eq "1" ];
  then
    info "[parseAndAppend] Files exist, skipping parsing for $VERSION_CURR"
    return
  fi

  parse_schema "$1" "$2"
}

sortAndGenerate()
{
  debug "sortAndGenerate for $SQL_SORTED::$SCRIPT_INSERT"
  if [ -e "$SQL_SORTED" ] && [ "$skipParsing" -eq "1" ];
  then
    debug "$SQL_SORTED exists and skipParsing is true, skipping sorting."
  else
    cleanse_sql
    sort_tabledefs "$1"
  fi

  if [ -e "$SQL_SORTED" ] && [ "$skipGeneration" -eq "0" ];
  then
    generate_data "$1"
  else
    debug "SQL file not found or skipGeneration is true, skipping generation..."
  fi
}

clearFiles() {
  rmfs "$SQL_OUT"
  rmfs "$SQL_SORTED"
  rmfs "$SCRIPT_INSERT"
  rmfs "$SCRIPT_QUERY"
}

info "Processing $HIVE_SCHEMA_BASE"
processFile "$HIVE_SCHEMA_BASE" "0"

if [[ -e "$SCRIPT_INSERT" ]]
  then
    debug "Appending file to master:$SCRIPT_INSERT"
    cat "$SCRIPT_INSERT" >> "$INSERTALL"
fi

FILE_LIST="$HMS_UPGRADE_DIR/$DB_SERVER/upgrade.order.$DB_SERVER"

prefix=""
  case "$DB_SERVER" in
    mysql)
      # TODO perform a case-insensitive match
      prefix="SOURCE"
      ;;
    postgres)
      prefix="i\ "
      ;;
    derby)
      # TODO perform a case-insensitive match
      prefix="RUN"
      ;;
    oracle)
      prefix="NOTSUPPORTED"
      ;;
    mssql)
      prefix="NOTSUPPORTED"
      ;;
  esac

# iterate thru every upgrade file listed in the upgrade.order.<db> file and for each
# upgrade file, parse the schema file and generate the data and a query file.
while read order
do
  curr="$VERSION_CURR-to-"
  if test "${order#*$curr}" != "$order"
  then
    VERSION_CURR="${order#*$curr}"
    SQL_OUT="/tmp/$DB_SERVER-$VERSION_CURR-create.sql"
    SQL_SORTED="/tmp/$DB_SERVER-$VERSION_CURR-sortedcreate.sql"
    SCRIPT_INSERT="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-$VERSION_CURR-insert.sql"
    SCRIPT_QUERY="$(pwd)/dbs/$DB_SERVER/$DB_SERVER-$VERSION_CURR-query.sql"
    upgrade="$HMS_UPGRADE_DIR/$DB_SERVER/upgrade-$order.$DB_SERVER.sql"
    append=0

    if [ -e $SQL_OUT ] && [ -e $SCRIPT_INSERT ] && [ -e $SQL_SORTED ] && [ "$skipParsing" -eq "1" ] && [ "$skipGeneration" -eq "1" ];
    then
      info "Files exist, Skipping parsing/generation for $VERSION_CURR"
      continue
    fi

    if [ "$skipParsing" -eq "0" ];
    then
      clearFiles
    elif [ "$skipGeneration" -eq "0" ];
    then
      rmfs "$SCRIPT_INSERT"
      rmfs "$SCRIPT_QUERY"
    fi

    while read line
    do
      debug "$line"
      if [[ $line =~ $prefix.* ]]
      then
        debug "upgrade.order processing $line prefix=$prefix"
        minisql=`echo $line | cut -d' ' -f2- | cut -d';' -f1`
        minisql="${minisql%\'}"
        minisql="${minisql#\'}"
        info "Processing $DB_SERVER/$minisql"
        parseAndAppend "$HMS_UPGRADE_DIR/$DB_SERVER/$minisql" "$append"
        append=1
      fi
    done < $upgrade;

    sortAndGenerate "$append"

    if [[ -e $SCRIPT_INSERT ]]
    then
      info "Appending file to master $SCRIPT_INSERT"
      cat "$SCRIPT_INSERT" >> "$INSERTALL"
    fi

    append=0
  fi
done < $FILE_LIST;

info "Hive Metastore data validation test successful."
