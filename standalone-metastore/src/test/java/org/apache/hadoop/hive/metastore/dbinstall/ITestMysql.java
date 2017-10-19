/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.dbinstall;

public class ITestMysql extends DbInstallBase {

  @Override
  protected String getDockerImageName() {
    return "mariadb:5.5";
  }

  @Override
  protected String[] getDockerAdditionalArgs() {
    return buildArray(
        "-p",
        "3306:3306",
        "-e",
        "MYSQL_ROOT_PASSWORD=" + getDbRootPassword(),
        "-d"
    );
  }

  @Override
  protected String getDbType() {
    return "mysql";
  }

  @Override
  protected String getDbRootUser() {
    return "root";
  }

  @Override
  protected String getDbRootPassword() {
    return "its-a-secret";
  }

  @Override
  protected String getJdbcDriver() {
    return org.mariadb.jdbc.Driver.class.getName();
  }

  @Override
  protected String getJdbcUrl() {
    return "jdbc:mysql://localhost:3306/" + HIVE_DB;
  }

  @Override
  protected String getInitialJdbcUrl() {
    return "jdbc:mysql://localhost:3306/";
  }

  @Override
  protected boolean isContainerReady(String logOutput) {
    return logOutput.contains("MySQL init process done. Ready for start up.");
  }

  @Override
  protected String getDockerContainerName() {
    return "metastore-test-mysql-install";
  }

  @Override
  protected String getHivePassword() {
    return "hivepassword";
  }
}
