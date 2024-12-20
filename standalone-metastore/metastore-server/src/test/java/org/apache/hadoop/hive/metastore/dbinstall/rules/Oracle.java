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
 * JUnit TestRule for Oracle.
 */
public class Oracle extends DatabaseRule {

  @Override
  public String getDockerImageName() {
    return "abstractdog/oracle-xe:18.4.0-slim";
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return buildArray(
        "-p",
        "1521:1521",
        "-d",
        "-e",
        "ORACLE_PASSWORD=" + getDbRootPassword()
    );
  }

  @Override
  public String getDbType() {
    return "oracle";
  }

  @Override
  public String getDbRootUser() {
    return "SYS as SYSDBA";
  }

  @Override
  public String getDbRootPassword() {
    return "oracle";
  }

  @Override
  public String getJdbcDriver() {
    return "oracle.jdbc.OracleDriver";
  }

  @Override
  public String getJdbcUrl(String hostAddress) {
    return "jdbc:oracle:thin:@//" + hostAddress + ":1521/xe";
  }

  @Override
  public String getInitialJdbcUrl(String hostAddress) {
    return "jdbc:oracle:thin:@//" + hostAddress + ":1521/xe";
  }

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    return pr.stdout.contains("DATABASE IS READY TO USE!");
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }

  @Override
  public String getHiveUser() {
    return "c##"+ super.getHiveUser();
  }
}
