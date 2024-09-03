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
set -x
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

SOURCE_DIR=${SOURCE_DIR:-"../../.."}
repo=${REPO:-apache}
WORK_DIR="$(mktemp -d)"
HADOOP_VERSION=${HADOOP_VERSION:-$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=hadoop.version -DforceStdout)}
TEZ_VERSION=${TEZ_VERSION:-$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=tez.version -DforceStdout)}

HADOOP_URL=${HADOOP_URL:-"https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"}
echo "Downloading Hadoop from $HADOOP_URL..."
if ! curl --fail -L "$HADOOP_URL" -o "$WORK_DIR/hadoop-$HADOOP_VERSION.tar.gz"; then
  echo "Fail to download Hadoop, exiting...."
  exit 1
fi

TEZ_URL=${TEZ_URL:-"https://archive.apache.org/dist/tez/$TEZ_VERSION/apache-tez-$TEZ_VERSION-bin.tar.gz"}
echo "Downloading Tez from $TEZ_URL..."
if ! curl --fail -L "$TEZ_URL" -o "$WORK_DIR/apache-tez-$TEZ_VERSION-bin.tar.gz"; then
  echo "Failed to download Tez, exiting..."
  exit 1
fi

if [ -n "$HIVE_VERSION" ]; then
  HIVE_URL=${HIVE_URL:-"https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz"}
  echo "Downloading Hive from $HIVE_URL..."
  if ! curl --fail -L "$HIVE_URL" -o "$WORK_DIR/apache-hive-$HIVE_VERSION-bin.tar.gz"; then
    echo "Failed to download Hive, exiting..."
    exit 1
  fi
  hive_package="$WORK_DIR/apache-hive-$HIVE_VERSION-bin.tar.gz"
else
    HIVE_VERSION=$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=project.version -DforceStdout)
    HIVE_TAR="$SOURCE_DIR/packaging/target/apache-hive-$HIVE_VERSION-bin.tar.gz"
    if  ls $HIVE_TAR || mvn -f $SOURCE_DIR/pom.xml clean package -DskipTests -Pdist; then
      cp "$HIVE_TAR" "$WORK_DIR/"
    else
      echo "Failed to compile Hive Project, exiting..."
      exit 1
    fi
fi

cp -R "$SOURCE_DIR/packaging/src/docker/conf" "$WORK_DIR/"
cp -R "$SOURCE_DIR/packaging/src/docker/entrypoint.sh" "$WORK_DIR/"
cp    "$SOURCE_DIR/packaging/src/docker/Dockerfile" "$WORK_DIR/"
docker build \
        "$WORK_DIR" \
        -f "$WORK_DIR/Dockerfile" \
        -t "$repo/hive:$HIVE_VERSION" \
        --build-arg "BUILD_ENV=unarchive" \
        --build-arg "HIVE_VERSION=$HIVE_VERSION" \
        --build-arg "HADOOP_VERSION=$HADOOP_VERSION" \
        --build-arg "TEZ_VERSION=$TEZ_VERSION" \

rm -r "${WORK_DIR}"
