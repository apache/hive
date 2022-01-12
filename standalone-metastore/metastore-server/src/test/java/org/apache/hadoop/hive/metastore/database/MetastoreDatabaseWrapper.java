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
package org.apache.hadoop.hive.metastore.database;

import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.hive.testutils.database.AbstractDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class MetastoreDatabaseWrapper {

  private final Logger LOG = LoggerFactory.getLogger(MetastoreDatabaseWrapper.class);

  protected AbstractDatabase database;

  public void before() throws Exception {
    database.launchDockerContainer();
    MetastoreSchemaTool.setHomeDirForTesting();
  }

  public void after() {
    if ("true".equalsIgnoreCase(System.getProperty("metastore.itest.no.stop.container"))) {
      LOG.warn("Not stopping container " + database.getDockerContainerName() + " at user request, please "
          + "be sure to shut it down before rerunning the test.");
      return;
    }
    try {
      database.cleanupDockerContainer();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  public AbstractDatabase getDatabase(){
    return database;
  }

  public void install() {
    createUser();
    installLatest();
  }

  public int createUser() {
    return new MetastoreSchemaTool().run(new String[] {
        "-createUser",
        "-dbType", database.getDbType(),
        "-userName", database.getDbRootUser(),
        "-passWord", database.getDbRootPassword(),
        "-hiveUser", database.getHiveUser(),
        "-hivePassword", database.getHivePassword(),
        "-hiveDb", database.getDb(),
        "-url", database.getInitialJdbcUrl(),
        "-driver", database.getJdbcDriver()
    });
  }

  public int installLatest() {
    return new MetastoreSchemaTool().run(new String[] {
        "-initSchema",
        "-dbType", database.getDbType(),
        "-userName", database.getHiveUser(),
        "-passWord", database.getHivePassword(),
        "-url", database.getJdbcUrl(),
        "-driver", database.getJdbcDriver(),
        "-verbose"
    });
  }

  public int upgradeToLatest() {
    return new MetastoreSchemaTool().run(new String[] {
        "-upgradeSchema",
        "-dbType", database.getDbType(),
        "-userName", database.getHiveUser(),
        "-passWord", database.getHivePassword(),
        "-url", database.getJdbcUrl(),
        "-driver", database.getJdbcDriver()
    });
  }

  public int validateSchema() {
    return new MetastoreSchemaTool().run(new String[] {
        "-validate",
        "-dbType", database.getDbType(),
        "-userName", database.getHiveUser(),
        "-passWord", database.getHivePassword(),
        "-url", database.getJdbcUrl(),
        "-driver", database.getJdbcDriver()
    });
  }

  public int installAVersion(String version) {
    return new MetastoreSchemaTool().run(new String[] {
        "-initSchemaTo", version,
        "-dbType", database.getDbType(),
        "-userName", database.getHiveUser(),
        "-passWord", database.getHivePassword(),
        "-url", database.getJdbcUrl(),
        "-driver", database.getJdbcDriver()
    });
  }

  public static MetastoreDatabaseWrapper getMetastoreDatabase(String metastoreType) {
    switch (metastoreType) {
    case "postgres":
      return new MetastorePostgres();
    case "postgres.tpcds":
      return new MetastorePostgresTPCDS();
    case "oracle":
      return new MetastoreOracle();
    case "mysql":
      return new MetastoreMysql();
    case "mariadb":
      return new MetastoreMariadb();
    case "mssql":
    case "sqlserver":
      return new MetastoreMssql();
    default:
      return new MetastoreDerby();
    }
  }
}
