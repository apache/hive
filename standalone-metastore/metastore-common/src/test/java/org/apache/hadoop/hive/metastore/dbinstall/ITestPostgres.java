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

public class ITestPostgres extends DbInstallBase {
  @Override
  protected String getDockerContainerName() {
    return "metastore-test-postgres-install";
  }

  @Override
  protected String getDockerImageName() {
    return "postgres:9.3";
  }

  @Override
  protected String[] getDockerAdditionalArgs() {
    return buildArray(
        "-p",
        "5432:5432",
        "-e",
        "POSTGRES_PASSWORD=" + getDbRootPassword(),
        "-d"

    );
  }

  @Override
  protected String getDbType() {
    return "postgres";
  }

  @Override
  protected String getDbRootUser() {
    return "postgres";
  }

  @Override
  protected String getDbRootPassword() {
    return "its-a-secret";
  }

  @Override
  protected String getJdbcDriver() {
    return org.postgresql.Driver.class.getName();
  }

  @Override
  protected String getJdbcUrl() {
    return "jdbc:postgresql://localhost:5432/" + HIVE_DB;
  }

  @Override
  protected String getInitialJdbcUrl() {
    return "jdbc:postgresql://localhost:5432/postgres";
  }

  @Override
  protected boolean isContainerReady(String logOutput) {
    return logOutput.contains("database system is ready to accept connections");
  }

  @Override
  protected String getHivePassword() {
    return "hivepassword";
  }
}
