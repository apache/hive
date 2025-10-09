/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.dbinstall.rules;

import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

public class Mariadb extends DatabaseRule {
  private final MariaDBContainer<?> container = new MariaDBContainer<>(DockerImageName.parse("mariadb:11.4"));

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
    return "mysql";
  }

  @Override
  public String getDbRootUser() {
    return "root";
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
    return container.withDatabaseName("").getJdbcUrl();
  }

  @Override
  public String getHivePassword() {
    return HIVE_PASSWORD;
  }
}
