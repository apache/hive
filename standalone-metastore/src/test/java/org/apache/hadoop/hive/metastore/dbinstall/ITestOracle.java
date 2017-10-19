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

public class ITestOracle extends DbInstallBase {
  @Override
  protected String getDockerContainerName() {
    return "metastore-test-oracle-install";
  }

  @Override
  protected String getDockerImageName() {
    return "alexeiled/docker-oracle-xe-11g";
  }

  @Override
  protected String[] getDockerAdditionalArgs() {
    return buildArray(
        "-p",
        "1521:1521",
        "-e",
        "DEFAULT_SYS_PASS=" + getDbRootPassword(),
        "-e",
        "ORACLE_ALLOW_REMOTE=true",
        "-d"
    );
  }

  @Override
  protected String getDbType() {
    return "oracle";
  }

  @Override
  protected String getDbRootUser() {
    return "SYS as SYSDBA";
  }

  @Override
  protected String getDbRootPassword() {
    return "oracle";
  }

  @Override
  protected String getJdbcDriver() {
    return "oracle.jdbc.OracleDriver";
  }

  @Override
  protected String getJdbcUrl() {
    return "jdbc:oracle:thin:@//localhost:1521/xe";
  }

  @Override
  protected String getInitialJdbcUrl() {
    return "jdbc:oracle:thin:@//localhost:1521/xe";
  }

  @Override
  protected boolean isContainerReady(String logOutput) {
    return logOutput.contains("Oracle started successfully!");
  }

  @Override
  protected String getHivePassword() {
    return "hivepassword";
  }
}
