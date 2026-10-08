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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.dbinstall.rules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * JUnit TestRule for Postgres metastore with TPCDS schema and stat information.
 */
public class PostgresTPCDS extends Postgres {
  public PostgresTPCDS() {
    super(DockerImageName.parse("docker.io/thomasrebele/postgres-tpcds-metastore:1.5")
        .asCompatibleSubstituteFor("postgres"));
    container.withUsername("postgres");
    // the image restores a raw database dump directly, so the database restore phase is skipped,
    // and the string only appears once in the logs
    container.waitingFor(
        new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\s").withTimes(1)
            .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS)));
  }

  @Override
  public String getJdbcUrl() {
    return container.withDatabaseName("metastore").getJdbcUrl();
  }

  @Override
  public String getHiveUser() {
    return "hive";
  }

  @Override
  public String getHivePassword() {
    return "hive";
  }

  @Override
  public void install() {
    // Upgrade the metastore to latest by running explicitly a script.
    String hiveSchemaVer = MetaStoreSchemaInfoFactory.get(new Configuration()).getHiveSchemaVersion();
    try (InputStream script = PostgresTPCDS.class.getClassLoader()
        .getResourceAsStream("sql/postgres/migrate-metastore-dump-to-" + hiveSchemaVer + ".postgres.sql")) {
      new MetastoreSchemaTool().runScript(
          buildArray(
              "-upgradeSchema",
              "-dbType", getDbType(),
              "-userName", getHiveUser(),
              "-passWord", getHivePassword(),
              "-url", getJdbcUrl(),
              "-driver", getJdbcDriver()),
          script);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }
}

