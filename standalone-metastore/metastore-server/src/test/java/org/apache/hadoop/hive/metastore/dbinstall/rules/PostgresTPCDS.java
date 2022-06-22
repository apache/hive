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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * JUnit TestRule for Postgres metastore with TPCDS schema and stat information.
 */
public class PostgresTPCDS extends Postgres {
  @Override
  public String getDockerImageName() {
    return "zabetak/postgres-tpcds-metastore:1.3";
  }

  @Override
  public String getJdbcUrl(String hostAddress) {
    return "jdbc:postgresql://" + hostAddress + ":5432/metastore";
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
        .getResourceAsStream("sql/postgres/upgrade-3.1.3000-to-" + hiveSchemaVer + ".postgres.sql")) {
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

