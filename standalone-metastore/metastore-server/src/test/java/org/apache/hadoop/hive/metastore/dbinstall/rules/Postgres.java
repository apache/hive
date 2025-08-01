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

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

/**
 * JUnit TestRule for Postgres.
 */
public class Postgres extends DatabaseRule {
  protected final PostgreSQLContainer<?> container;

  public Postgres() {
    this(DockerImageName.parse("postgres:11.4"));
  }

  protected Postgres(DockerImageName dockerImageName) {
   this.container = new PostgreSQLContainer<>(dockerImageName);
  }

  @Override
  public void before() throws IOException, InterruptedException {
    container.start();
  }

  @Override
  public void after() {
    container.stop();
  }

  @Override
  public String getDbType() {
    return "postgres";
  }

  @Override
  public String getDbRootUser() {
    return container.getUsername();
  }

  @Override
  public String getDbRootPassword() {
    return container.getPassword();
  }

  @Override
  public String getJdbcDriver() {
    return container.getDriverClassName();
  }

  @Override
  public String getJdbcUrl() {
    return container.withDatabaseName(HIVE_DB).getJdbcUrl();
  }

  @Override
  public String getInitialJdbcUrl() {
    return container.withDatabaseName("postgres").getJdbcUrl();
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
