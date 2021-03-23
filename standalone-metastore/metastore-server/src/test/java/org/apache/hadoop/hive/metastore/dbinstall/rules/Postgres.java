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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit TestRule for Postgres.
 */
public class Postgres extends DatabaseRule {
  private static final Logger LOG = LoggerFactory.getLogger(Postgres.class);

  @Override
  public String getDockerImageName() {
    return "postgres:9.3";
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
    if (pr.stdout.contains("PostgreSQL init process complete; ready for start up")) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(getContainerHostAddress(), 5432), 1000);
        return true;
      } catch (IOException e) {
        LOG.info("cant connect to postgres; {}", e.getMessage());
        return false;
      }
    }
    return false;
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
