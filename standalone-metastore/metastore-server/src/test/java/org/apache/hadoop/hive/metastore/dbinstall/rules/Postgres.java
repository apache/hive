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
 * JUnit TestRule for Postgres.
 */
public class Postgres extends DatabaseRule {
  @Override
  public String getDockerImageName() {
    return "postgres:11.6";
  }

  @Override
  public String[] getDockerAdditionalArgs() {
    return buildArray("-p", "5432:5432", "-e", "POSTGRES_PASSWORD=" + getDbRootPassword(), "-d");
  }

  @Override
  public String getDbType() {
    return "postgres";
  }

  @Override
  public String getDbRootUser() {
    return "postgres";
  }

  @Override
  public String getDbRootPassword() {
    return "its-a-secret";
  }

  @Override
  public String getJdbcDriver() {
    return org.postgresql.Driver.class.getName();
  }

  @Override
  public String getJdbcUrl(String hostAddress) {
    return "jdbc:postgresql://" + hostAddress + ":5432/" + HIVE_DB;
  }

  @Override
  public String getInitialJdbcUrl(String hostAddress) {
    return "jdbc:postgresql://" + hostAddress + ":5432/postgres";
  }

  @Override
  public boolean isContainerReady(ProcessResults pr) {
    return pr.stdout.contains("database system is ready to accept connections") &&
        pr.stderr.contains("database system is ready to accept connections");
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
