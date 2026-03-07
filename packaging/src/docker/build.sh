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
set -eux
HIVE_VERSION=
HADOOP_VERSION=
TEZ_VERSION=
usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-hadoop <Hadoop version>] [-tez <Tez version>] [-hive <Hive version>] [-repo <Docker repo>]
Build the Hive Docker image
-help                Display help
-hadoop              Build image with the specified Hadoop version
-tez                 Build image with the specified Tez version
-hive                Build image with the specified Hive version
-repo                Docker repository
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h)
       usage
       exit 0
       ;;
    -hadoop)
      shift
      HADOOP_VERSION=$1
      shift
      ;;
    -tez)
      shift
      TEZ_VERSION=$1
      shift
      ;;
    -hive)
      shift
      HIVE_VERSION=$1
      shift
      ;;
    -repo)
      shift
      REPO=$1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

SCRIPT_DIR=$(cd $(dirname $0); pwd)
SOURCE_DIR=${SOURCE_DIR:-"$SCRIPT_DIR/../../.."}
repo=${REPO:-apache}
WORK_DIR="$(mktemp -d)"
CACHE_DIR="$SCRIPT_DIR/../../cache"
mkdir -p "$CACHE_DIR"
HADOOP_VERSION=${HADOOP_VERSION:-$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=hadoop.version -DforceStdout)}
TEZ_VERSION=${TEZ_VERSION:-$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=tez.version -DforceStdout)}

HADOOP_FILE_NAME="hadoop-$HADOOP_VERSION.tar.gz"
HADOOP_URL=${HADOOP_URL:-"https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$HADOOP_FILE_NAME"}
if [ ! -f "$CACHE_DIR/$HADOOP_FILE_NAME" ]; then
  echo "Downloading Hadoop from $HADOOP_URL..."
  if ! curl --fail -L "$HADOOP_URL" -o "$CACHE_DIR/$HADOOP_FILE_NAME.tmp"; then
    echo "Fail to download Hadoop, exiting...."
    exit 1
  fi
  mv "$CACHE_DIR/$HADOOP_FILE_NAME.tmp" "$CACHE_DIR/$HADOOP_FILE_NAME"
fi

TEZ_FILE_NAME="apache-tez-$TEZ_VERSION-bin.tar.gz"
TEZ_URL=${TEZ_URL:-"https://downloads.apache.org/tez/$TEZ_VERSION/$TEZ_FILE_NAME"}
if [ ! -f "$CACHE_DIR/$TEZ_FILE_NAME" ]; then
  echo "Downloading Tez from $TEZ_URL..."
  if ! curl --fail -L "$TEZ_URL" -o "$CACHE_DIR/$TEZ_FILE_NAME.tmp"; then
    echo "Failed to download Tez, exiting..."
    exit 1
  fi
  mv "$CACHE_DIR/$TEZ_FILE_NAME.tmp" "$CACHE_DIR/$TEZ_FILE_NAME"
fi

if [ -n "$HIVE_VERSION" ]; then
  HIVE_FILE_NAME="apache-hive-$HIVE_VERSION-bin.tar.gz"
  if [ ! -f "$CACHE_DIR/$HIVE_FILE_NAME" ]; then
    HIVE_URL=${HIVE_URL:-"https://downloads.apache.org/hive/hive-$HIVE_VERSION/$HIVE_FILE_NAME"}
    echo "Downloading Hive from $HIVE_URL..."
    if ! curl --fail -L "$HIVE_URL" -o "$CACHE_DIR/$HIVE_FILE_NAME.tmp"; then
      echo "Failed to download Hive, exiting..."
      exit 1
    fi
    mv "$CACHE_DIR/$HIVE_FILE_NAME.tmp" "$CACHE_DIR/$HIVE_FILE_NAME"
  fi
  cp "$CACHE_DIR/$HIVE_FILE_NAME" "$WORK_DIR"
else
  HIVE_VERSION=$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=project.version -DforceStdout)
  HIVE_TAR="$SOURCE_DIR/packaging/target/apache-hive-$HIVE_VERSION-bin.tar.gz"
  if  ls "$HIVE_TAR" || mvn -f "$SOURCE_DIR/pom.xml" clean package -DskipTests -Pdist; then
    cp "$HIVE_TAR" "$WORK_DIR/"
  else
    echo "Failed to compile Hive Project, exiting..."
    exit 1
  fi
fi

cp "$CACHE_DIR/hadoop-$HADOOP_VERSION.tar.gz" "$WORK_DIR/"
cp "$CACHE_DIR/apache-tez-$TEZ_VERSION-bin.tar.gz" "$WORK_DIR/"
cp -R "$SOURCE_DIR/packaging/src/docker/conf" "$WORK_DIR/"
cp -R "$SOURCE_DIR/packaging/src/docker/entrypoint.sh" "$WORK_DIR/"
cp    "$SOURCE_DIR/packaging/src/docker/Dockerfile" "$WORK_DIR/"
docker build \
        "$WORK_DIR" \
        -f "$WORK_DIR/Dockerfile" \
        -t "$repo/hive:$HIVE_VERSION" \
        --build-arg "HIVE_VERSION=$HIVE_VERSION" \
        --build-arg "HADOOP_VERSION=$HADOOP_VERSION" \
        --build-arg "TEZ_VERSION=$TEZ_VERSION" \

rm -r "${WORK_DIR}"
