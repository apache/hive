#!/bin/bash

if [ -z "$GPG_PASSPHRASE" ]; then
    echo "Need to set GPG_PASSPHRASE"
    exit 1
fi

PUBLISH_PROFILES="-Phadoop-2 -DskipTests -Psources -Pjavadoc"

echo "Replacing groupID with org.spark-project.hive"
find . -name pom.xml | \
  xargs -I {} sed -i -e "s/org.apache.hive/org.spark-project.hive/g" {}

echo "Publishing Spark to OSS Sonatype"
mvn clean deploy -Dgpg.passphrase=$GPG_PASSPHRASE $PUBLISH_PROFILES
