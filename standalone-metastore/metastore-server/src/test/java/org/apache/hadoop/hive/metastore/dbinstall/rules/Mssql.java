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
package org.apache.hadoop.hive.metastore.dbinstall.rules;

/**
 * JUnit TestRule for Mssql.
 */
public class Mssql extends DatabaseRule {

  @Override
  public String getDockerImageName() {
    return "mcr.microsoft.com/mssql/server:2019-latest";
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return buildArray(
        "-p",
        "1433:1433",
        "-e",
        "ACCEPT_EULA=Y",
        "-e",
        "SA_PASSWORD=" + getDbRootPassword(),
        "-d"
    );
  }

  @Override
  public String getDbType() {
    return "mssql";
  }

  @Override
  public String getDbRootUser() {
    return "SA";
  }

  @Override
  public String getDbRootPassword() {
    return "Its-a-s3cret";
  }

  @Override
  public String getJdbcDriver() {
    return com.microsoft.sqlserver.jdbc.SQLServerDriver.class.getName();
    // return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  }

  @Override
  public String getJdbcUrl(String hostAddress) {
    return "jdbc:sqlserver://" + hostAddress + ":1433;DatabaseName=" + HIVE_DB + ";";
  }

  @Override
  public String getInitialJdbcUrl(String hostAddress) {
    return "jdbc:sqlserver://" + hostAddress + ":1433";
  }

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    return pr.stdout
        .contains(
        "Recovery is complete. This is an informational message only. No user action is required.");
  }

  @Override
  public String getHivePassword() {
    return "h1vePassword!";
  }
}
