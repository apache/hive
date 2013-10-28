#!/bin/bash
function delete() {
  rm -rf "$@"
}
delete build.properties
delete build.xml
delete build-common.xml
delete build-offline.xml
delete ant/build.xml
delete beeline/build.xml
delete cli/build.xml
delete common/build.xml
delete contrib/build.xml
delete hbase-handler/build.xml
delete hcatalog/build.xml
delete hcatalog/core/build.xml
delete hcatalog/hcatalog-pig-adapter/build.xml
delete hcatalog/server-extensions/build.xml
delete hcatalog/src/test/e2e/hcatalog/build.xml
delete hcatalog/src/test/e2e/hcatalog/tools/generate/java/build.xml
delete hcatalog/src/test/e2e/hcatalog/udfs/java/build.xml
delete hcatalog/src/test/e2e/templeton/build.xml
delete hcatalog/storage-handlers/hbase/build.xml
delete hcatalog/webhcat/java-client/build.xml
delete hcatalog/webhcat/svr/build.xml
delete hwi/build.xml
delete jdbc/build.xml
delete metastore/build.xml
delete odbc/build.xml
delete ql/build.xml
delete serde/build.xml
delete service/build.xml
delete shims/build.xml
delete testutils/build.xml
delete hcatalog/core/pom-old.xml
delete hcatalog/hcatalog-pig-adapter/pom-old.xml
delete hcatalog/pom-old.xml
delete hcatalog/server-extensions/pom-old.xml
delete hcatalog/storage-handlers/hbase/pom-old.xml
delete hcatalog/webhcat/java-client/pom-old.xml
delete hcatalog/webhcat/svr/pom-old.xml
delete hcatalog/build-support/ant/build-command.xml
delete hcatalog/build-support/ant/deploy.xml
delete hcatalog/build-support/ant/test.xml
delete ivy
delete maven-rollback.sh
delete maven-rollforward.sh
delete maven-delete-ant.sh
