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
package org.apache.hadoop.hive.ql.qoption;

import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.externalDB.AbstractExternalDB;
import org.apache.hadoop.hive.ql.externalDB.MSSQLServer;
import org.apache.hadoop.hive.ql.externalDB.MariaDB;
import org.apache.hadoop.hive.ql.externalDB.MySQLExternalDB;
import org.apache.hadoop.hive.ql.externalDB.Oracle;
import org.apache.hadoop.hive.ql.externalDB.PostgresExternalDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An option handler for spinning (resp. stopping) databases before (resp. after) running a test.
 *
 * Syntax: qt:database:DatabaseType:databaseName:path_to_init_script
 *
 * The database type ({@link DatabaseType}) and the database name are obligatory arguments. The initialization script can be omitted.
 * The database name is mainly used to distinguish between different databases when they are accessed via a system {@link AbstractExternalDB.ConnectionProperty}.
 * Current limitations:
 * <ol>
 *   <li>Only one test/file per run</li>
 *   <li>Does not support parallel execution</li>
 *   <li>Cannot instantiate more than one database of the same type per test</li>
 * </ol>
 */
public class QTestDatabaseHandler implements QTestOptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(QTestDatabaseHandler.class);

  private enum DatabaseType {
    POSTGRES {
      @Override
      AbstractExternalDB create() {
        return new PostgresExternalDB();
      }
    }, MYSQL {
      @Override
      AbstractExternalDB create() {
        return new MySQLExternalDB();
      }
    }, MARIADB {
      @Override
      AbstractExternalDB create() {
        return new MariaDB();
      }
    }, MSSQL {
      @Override
      AbstractExternalDB create() {
        return new MSSQLServer();
      }
    }, ORACLE {
      @Override
      AbstractExternalDB create() {
        return new Oracle();
      }
    };

    abstract AbstractExternalDB create();
  }

  private final String scriptsDir;
  private final List<AbstractExternalDB> databases = new ArrayList<>();

  public QTestDatabaseHandler(final String scriptDirectory) {
    this.scriptsDir = scriptDirectory;
  }

  @Override
  public void processArguments(String arguments) {
    String[] args = arguments.split(":");
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException(
          "Wrong number of arguments. Expected {dbtype:dbname:script?}. Found: " + Arrays.toString(args));
    }
    DatabaseType dbType = DatabaseType.valueOf(args[0].toUpperCase());
    String dbName = args[1];
    String initScript = args.length == 3 ? args[2] : null;
    AbstractExternalDB db = dbType.create();
    db.setName(dbName);
    if (initScript != null && !initScript.isEmpty()) {
      db.setInitScript(Paths.get(scriptsDir, initScript));
    }
    databases.add(db);
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    for (AbstractExternalDB db : databases) {
      db.start();
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    for (AbstractExternalDB db : databases) {
      try {
        db.stop();
      } catch (Exception e) {
        LOG.error("Failed to cleanup database {}", db, e);
      }
    }
    databases.clear();
  }

}
