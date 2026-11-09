#!/usr/bin/env bash
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

# Initializes an empty metastore, and the Hive SYS/INFORMATION_SCHEMA databases on top of it, with
# bin/schematool of the binary distribution built by "mvn install -Pdist".
# Used by the init@<db> stages of the Jenkinsfile, in the precommit image (dev-support/docker/precommit).
#
# Usage: dev-support/precommit-init-metastore.sh <derby|postgres>
#
# Environment:
#   HADOOP_HOME     Hadoop installation used by bin/schematool (default: /opt/hadoop)
#   MAVEN_ARGS      extra Maven arguments (settings, local repository) used to fetch the JDBC driver
#   DOCKER_NETWORK  network of the database container (default: host); its port must be
#                   reachable on localhost
#   POSTGRES_IMAGE  image of the postgres container (default: postgres:17-alpine)

set -euo pipefail

dbType=${1:?usage: $0 <derby|postgres>}

cd "$(dirname "$0")/.."

hiveHomes=(packaging/target/apache-hive-*-bin/apache-hive-*-bin)
if [[ ! -d ${hiveHomes[0]} ]]; then
  echo "No Hive binary distribution under packaging/target; build it with -Pdist first." >&2
  exit 1
fi
export HIVE_HOME=$PWD/${hiveHomes[0]}
export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} --add-opens java.base/java.net=ALL-UNNAMED"

work=$PWD/target/init-metastore/$dbType
rm -rf "$work"
mkdir -p "$work/conf" "$work/lib"
export HIVE_CONF_DIR=$work/conf
export HADOOP_CLIENT_OPTS+=" -Dderby.stream.error.file=$work/derby.log"
export HADOOP_CLASSPATH="$work/lib/*"

dbContainer=
cleanup() {
  if [[ -n $dbContainer ]]; then
    docker logs "$dbContainer" > "$work/$dbContainer.log" 2>&1 || true
    docker rm -f -v "$dbContainer" > /dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

pomProperty() {
  xmlstarlet sel -t -v "/_:project/_:properties/_:$1" pom.xml
}

case $dbType in
  derby)
    driver=org.apache.derby.jdbc.EmbeddedDriver
    url="jdbc:derby:;databaseName=$work/metastore_db;create=true"
    user=APP
    password=mine
    ;;
  postgres)
    mvn -B -q dependency:copy -Dartifact="org.postgresql:postgresql:$(pomProperty postgres.version)" \
      -DoutputDirectory="$work/lib"
    dbContainer=hive-precommit-postgres
    docker rm -f -v "$dbContainer" > /dev/null 2>&1 || true
    docker run -d --name "$dbContainer" --network "${DOCKER_NETWORK:-host}" \
      -e POSTGRES_USER=hive -e POSTGRES_PASSWORD=hive -e POSTGRES_DB=metastore \
      "${POSTGRES_IMAGE:-postgres:17-alpine}" > /dev/null
    # Probe over TCP: during its first start the entrypoint runs a temporary server on the unix socket only.
    for ((i = 0; i < 120; i++)); do
      docker exec "$dbContainer" pg_isready -q -h 127.0.0.1 -U hive -d metastore && break
      sleep 1
    done
    docker exec "$dbContainer" pg_isready -h 127.0.0.1 -U hive -d metastore
    driver=org.postgresql.Driver
    url=jdbc:postgresql://localhost:5432/metastore
    user=hive
    password=hive
    ;;
  *)
    echo "Unsupported database type: $dbType" >&2
    exit 1
    ;;
esac

property() {
  printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"
}
{
  echo '<configuration>'
  property javax.jdo.option.ConnectionDriverName "$driver"
  property javax.jdo.option.ConnectionURL "$url"
  property javax.jdo.option.ConnectionUserName "$user"
  property javax.jdo.option.ConnectionPassword "$password"
  property hive.metastore.warehouse.dir "file://$work/warehouse"
  property hive.metastore.warehouse.external.dir "file://$work/warehouse-external"
  property hive.exec.scratchdir "$work/scratch"
  property hive.exec.local.scratchdir "$work/local-scratch"
  property hive.querylog.location "$work/querylog"
  property hive.server2.logging.operation.log.location "$work/operation-logs"
  property hive.user.install.directory "file://$work/user"
  echo '</configuration>'
} > "$HIVE_CONF_DIR/hive-site.xml"

"$HIVE_HOME/bin/schematool" -verbose -dbType "$dbType" -initSchema
"$HIVE_HOME/bin/schematool" -verbose -dbType hive -initSchema -metaDbType "$dbType"
echo "Metastore schema initialized on $dbType"
