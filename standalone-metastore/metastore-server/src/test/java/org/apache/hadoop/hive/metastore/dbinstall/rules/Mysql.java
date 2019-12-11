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
 * JUnit TestRule for MySql.
 */
public class Mysql extends DatabaseRule {

  @Override
  public String getDockerImageName() {
    return "mariadb:5.5";
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return buildArray("-p", "3306:3306", "-e", "MYSQL_ROOT_PASSWORD=" + getDbRootPassword(), "-d");
  }

  @Override
  public String getDbType() {
    return "mysql";
  }

  @Override
  public String getDbRootUser() {
    return "root";
  }

  @Override
  public String getDbRootPassword() {
    return "its-a-secret";
  }

  @Override
  public String getJdbcDriver() {
    return org.mariadb.jdbc.Driver.class.getName();
  }

  @Override
  public String getJdbcUrl() {
    return "jdbc:mysql://localhost:3306/" + HIVE_DB;
  }

  @Override
  public String getInitialJdbcUrl() {
    return "jdbc:mysql://localhost:3306/";
  }

  @Override
  public boolean isContainerReady(String logOutput) {
    return logOutput.contains("MySQL init process done. Ready for start up.");
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
