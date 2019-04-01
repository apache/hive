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

package org.apache.hadoop.hive.metastore.tools.schematool;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.Warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * {@link SchemaToolTaskDrop} drops all data from Hive. It invokes DROP TABLE on all
 * tables of the default database and DROP DATABASE CASCADE on all other databases.
 */
public class SchemaToolTaskDrop extends SchemaToolTask {

  @VisibleForTesting
  boolean yes = false;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cmdLine) {
    if (cmdLine.hasOption("yes")) {
      this.yes = true;
    }
  }

  @Override
  void execute() throws HiveMetaException {
    // Need to confirm unless it's a dry run or specified -yes
    if (!schemaTool.isDryRun() && !this.yes) {
      boolean confirmed = promptToConfirm();
      if (!confirmed) {
        System.out.println("Operation cancelled, exiting.");
        return;
      }
    }

    Connection conn = schemaTool.getConnectionToMetastore(true);
    try {
      try (Statement stmt = conn.createStatement()) {
        final String def = Warehouse.DEFAULT_DATABASE_NAME;

        // List databases
        List<String> databases = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
          while (rs.next()) {
            databases.add(rs.getString(1));
          }
        }

        // Drop databases
        for (String database : databases) {
          // Don't try to drop 'default' database as it's not allowed
          if (!def.equalsIgnoreCase(database)) {
            if (schemaTool.isDryRun()) {
              System.out.println("would drop database " + database);
            } else {
              logIfVerbose("dropping database " + database);
              stmt.execute(String.format("DROP DATABASE `%s` CASCADE", database));
            }
          }
        }

        // List tables in 'default' database
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(String.format("SHOW TABLES IN `%s`", def))) {
          while (rs.next()) {
            tables.add(rs.getString(1));
          }
        }

        // Drop tables in 'default' database
        for (String table : tables) {
          if (schemaTool.isDryRun()) {
            System.out.println("would drop table " + table);
          } else {
            logIfVerbose("dropping table " + table);
            stmt.execute(String.format("DROP TABLE `%s`.`%s`", def, table));
          }
        }
      }
    } catch (SQLException se) {
      throw new HiveMetaException("Failed to drop databases.", se);
    }
  }

  /**
   * Display "are you sure? y/n" on command line and return what the user has chosen
   * @return
   */
  private boolean promptToConfirm() {
    System.out.print("This operation will delete ALL managed data in Hive. " +
            "Are you sure you want to continue (y/[n])?");
    Scanner scanner = new Scanner(System.in, "UTF-8");
    String input = scanner.nextLine();

    if ("y".equalsIgnoreCase(input) || "yes".equalsIgnoreCase(input)) {
      return true;
    }
    return false;
  }

  private void logIfVerbose(String msg) {
    if (schemaTool.isVerbose()) {
      System.out.println(msg);
    }
  }
}
