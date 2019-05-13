#!/bin/bash

set -e
set -x

cd $(dirname $0)

if [ -n "$1" ]; then
  export CDH_GBN="$1"
else
  export CDH_GBN=$(curl 'http://builddb.infra.cloudera.com:8080/query?product=cdh&user=jenkins&version=6.x&tag=official')
fi

curl https://github.mtv.cloudera.com/raw/CDH/cdh/cdh6.x/gbn-m2-settings.xml -o /tmp/gbn-m2-settings.xml
sed "s/\\\${env.CDH_GBN}/$CDH_GBN/g" -i /tmp/gbn-m2-settings.xml

cd ..
mvn clean install -DskipTests -s /tmp/gbn-m2-settings.xml
cd itests
mvn clean install -DskipTests -s /tmp/gbn-m2-settings.xml
