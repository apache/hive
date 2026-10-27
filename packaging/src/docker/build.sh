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
TEZ_SNAPSHOT_VERSION=
AWS_SDK=auto
usage() {
    cat <<EOF 1>&2
Usage: $0 [-h] [-hadoop <Hadoop version>] -tez <Tez release version> [-tez-snapshot [<Maven snapshot version>]] [-hive <Hive version>] [-aws-sdk auto|none|<version>] [-repo <Docker repo>]
Build the Hive Docker image (reused for LLAP too)
-help                Display help
-hadoop              Build image with the specified Hadoop version (default: from Maven pom)
-tez                 Required. Tez release tarball version (apache-tez-\$TEZ_VERSION-bin.tar.gz from archive)
-tez-snapshot <ver>  Optional. When a snapshot version is given, the Tez Maven snapshot replaces the Tez release in the image. With no version, the release is used.
-hive                Build image with the specified Hive version
-aws-sdk <value>     Optional. AWS SDK v2 bundle for s3a:// support. One of:
                       auto      (default) aws-java-sdk.version from the Maven pom
                       none      no bundle in the image, keeping it ~650MB smaller. s3a:// paths then fail at runtime.
                       <version> an explicit software.amazon.awssdk:bundle version
                     Hadoop releases that ship their own bundle (up to 3.4.2) keep it and get no other.
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
    -tez-snapshot)
      shift
      if [ $# -gt 0 ] && [[ "$1" != -* ]]; then
        TEZ_SNAPSHOT_VERSION=$1
        shift
      fi
      ;;
    -hive)
      shift
      HIVE_VERSION=$1
      shift
      ;;
    -aws-sdk)
      shift
      AWS_SDK=$1
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

if [ -z "${TEZ_VERSION}" ]; then
  echo "Error: -tez <Tez version> is required." >&2
  usage
  exit 1
fi

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
TEZ_URL=${TEZ_URL:-"https://archive.apache.org/dist/tez/$TEZ_VERSION/$TEZ_FILE_NAME"}
if [ ! -f "$CACHE_DIR/$TEZ_FILE_NAME" ]; then
  echo "Downloading Tez from $TEZ_URL..."
  if ! curl --fail -L "$TEZ_URL" -o "$CACHE_DIR/$TEZ_FILE_NAME.tmp"; then
    echo "Failed to download Tez, exiting..."
    exit 1
  fi
  mv "$CACHE_DIR/$TEZ_FILE_NAME.tmp" "$CACHE_DIR/$TEZ_FILE_NAME"
fi

# The Dockerfile adds the AWS SDK v2 bundle for s3a:// (~650MB, hence the opt-out).
# It is cached here so that local builds do not download it every time.
case "$AWS_SDK" in
  auto)
    AWS_SDK_VERSION=$(mvn -f "$SOURCE_DIR/pom.xml" -q help:evaluate -Dexpression=aws-java-sdk.version -DforceStdout)
    ;;
  none)
    AWS_SDK_VERSION=
    ;;
  *)
    AWS_SDK_VERSION=$AWS_SDK
    ;;
esac

# Releases up to 3.4.2 ship their own bundle, which the Dockerfile keeps instead.
if [ -n "$AWS_SDK_VERSION" ] && ! tar -tzf "$CACHE_DIR/$HADOOP_FILE_NAME" | grep -q 'share/hadoop/.*/bundle-.*\.jar'; then
  AWS_SDK_FILE_NAME="bundle-$AWS_SDK_VERSION.jar"
  AWS_SDK_URL=${AWS_SDK_URL:-"https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/$AWS_SDK_VERSION/$AWS_SDK_FILE_NAME"}
  if [ ! -f "$CACHE_DIR/$AWS_SDK_FILE_NAME" ]; then
    echo "Downloading AWS SDK v2 bundle from $AWS_SDK_URL..."
    if ! curl --fail -L "$AWS_SDK_URL" -o "$CACHE_DIR/$AWS_SDK_FILE_NAME.tmp"; then
      echo "Failed to download the AWS SDK v2 bundle, exiting..."
      exit 1
    fi
    mv "$CACHE_DIR/$AWS_SDK_FILE_NAME.tmp" "$CACHE_DIR/$AWS_SDK_FILE_NAME"
  fi
  mkdir -p "$WORK_DIR/aws-jars"
  cp "$CACHE_DIR/$AWS_SDK_FILE_NAME" "$WORK_DIR/aws-jars/"
fi

if [ -n "$HIVE_VERSION" ]; then
  HIVE_FILE_NAME="apache-hive-$HIVE_VERSION-bin.tar.gz"
  if [ ! -f "$CACHE_DIR/$HIVE_FILE_NAME" ]; then
    HIVE_URL=${HIVE_URL:-"https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/$HIVE_FILE_NAME"}
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

DOCKER_BUILD_ARGS=(
  --build-arg "HIVE_VERSION=$HIVE_VERSION"
  --build-arg "HADOOP_VERSION=$HADOOP_VERSION"
  --build-arg "TEZ_VERSION=$TEZ_VERSION"
  --build-arg "AWS_SDK_VERSION=$AWS_SDK_VERSION"
)
if [ -n "$TEZ_SNAPSHOT_VERSION" ]; then
  DOCKER_BUILD_ARGS+=(--build-arg "TEZ_SNAPSHOT_VERSION=$TEZ_SNAPSHOT_VERSION")
fi

docker build \
        "$WORK_DIR" \
        -f "$WORK_DIR/Dockerfile" \
        -t "$repo/hive:$HIVE_VERSION" \
        "${DOCKER_BUILD_ARGS[@]}"

rm -r "${WORK_DIR}"
