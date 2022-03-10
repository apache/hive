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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

/**
 * An option handler for spinning (resp. stopping) databases before (resp. after) running a test.
 *
 * Syntax: qt:database:DatabaseType:path_to_init_script
 *
 * The database type ({@link DatabaseType}) is obligatory argument. The initialization script can be omitted.
 *
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

  private final Map<DatabaseType, String> databaseToScript = new EnumMap<>(DatabaseType.class);

  @Override
  public void processArguments(String arguments) {
    String[] args = arguments.split(":");
    if (args.length == 0) {
      throw new IllegalArgumentException("No arguments provided");
    }
    if (args.length > 2) {
      throw new IllegalArgumentException(
          "Too many arguments. Expected {dbtype:script}. Found: " + Arrays.toString(args));
    }
    DatabaseType dbType = DatabaseType.valueOf(args[0].toUpperCase());
    String initScript = args.length == 2 ? args[1] : "";
    if (databaseToScript.containsKey(dbType)) {
      throw new IllegalArgumentException(dbType + " database is already defined in this file.");
    }
    databaseToScript.put(dbType, initScript);
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    for (Map.Entry<DatabaseType, String> dbEntry : databaseToScript.entrySet()) {
      String scriptsDir = QTestUtil.getScriptsDir(qt.getConf());
      Path dbScript = Paths.get(scriptsDir, dbEntry.getValue());
      AbstractExternalDB db = dbEntry.getKey().create();
      db.launchDockerContainer();
      if (Files.exists(dbScript)) {
        db.execute(dbScript.toString());
      } else {
        LOG.warn("Initialization script {} not found", dbScript);
      }
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    for (Map.Entry<DatabaseType, String> dbEntry : databaseToScript.entrySet()) {
      AbstractExternalDB db = dbEntry.getKey().create();
      try {
        db.cleanupDockerContainer();
      } catch (Exception e) {
        LOG.error("Failed to cleanup database {}", dbEntry.getKey(), e);
      }
    }
    databaseToScript.clear();
  }

}
