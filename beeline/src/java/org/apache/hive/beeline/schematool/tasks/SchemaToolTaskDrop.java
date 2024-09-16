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

package org.apache.hive.beeline.schematool.tasks;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

/**
 * {@link SchemaToolTaskDrop} drops all data from Hive. It invokes DROP TABLE on all
 * tables of the default database and DROP DATABASE CASCADE on all other databases.
 */
class SchemaToolTaskDrop extends SchemaToolTask {

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("yes", "dryRun", "verbose");
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    boolean yes = commandLine.hasOption("yes");
    boolean dryRun = commandLine.hasOption("dryRun");
    boolean verbose = commandLine.hasOption("verbose");

    // Need to confirm unless it's a dry run or specified -yes
    if (!dryRun && !yes) {
      boolean confirmed = promptToConfirm();
      if (!confirmed) {
        System.out.println("Operation cancelled, exiting.");
        return;
      }
    }

    Connection conn = context.getConnectionToMetastore(true);
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
            if (dryRun) {
              System.out.println("would drop database " + database);
            } else {
              logIfVerbose("dropping database " + database, verbose);
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
          if (dryRun) {
            System.out.println("would drop table " + table);
          } else {
            logIfVerbose("dropping table " + table, verbose);
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

    return "y".equalsIgnoreCase(input) || "yes".equalsIgnoreCase(input);
  }

  private void logIfVerbose(String msg, boolean verbose) {
    if (verbose) {
      System.out.println(msg);
    }
  }

}
