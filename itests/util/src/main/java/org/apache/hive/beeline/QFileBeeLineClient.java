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

package org.apache.hive.beeline;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hive.beeline.ConvertedOutputFile.Converter;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * QFile test client using BeeLine. It can be used to submit a list of command strings, or a QFile.
 */
public class QFileBeeLineClient implements AutoCloseable {
  private BeeLine beeLine;
  private PrintStream beelineOutputStream;
  private File logFile;
  private String[] TEST_FIRST_COMMANDS = new String[] {
    "!set outputformat tsv2",
    "!set verbose false",
    "!set silent true",
    "!set showheader false",
    "!set escapeCRLF false",
    "USE default;",
    "SHOW TABLES;",
  };
  private String[] TEST_SET_LOG_COMMANDS = new String[] {
    "set hive.testing.short.logs=true;",
    "set hive.testing.remove.logs=false;",
  };
  private String[] TEST_RESET_COMMANDS = new String[] {
    "set hive.testing.short.logs=false;",
    "!set verbose true",
    "!set silent false",
    "!set showheader true",
    "!set escapeCRLF false",
    "!set outputformat table",
    "USE default;"
  };

  protected QFileBeeLineClient(String jdbcUrl, String jdbcDriver, String username, String password,
      File log) throws IOException {
    logFile = log;
    beeLine = new BeeLine();
    beelineOutputStream = new PrintStream(logFile, "UTF-8");
    beeLine.setOutputStream(beelineOutputStream);
    beeLine.setErrorStream(beelineOutputStream);
    beeLine.runCommands(
        new String[] {
          "!set verbose true",
          "!set shownestederrs true",
          "!set showwarnings true",
          "!set showelapsedtime false",
          "!set trimscripts false",
          "!set maxwidth -1",
          "!connect " + jdbcUrl + " " + username + " " + password + " " + jdbcDriver
        });
  }

  private Set<String> getDatabases() throws SQLException {
    Set<String> databases = new HashSet<String>();

    DatabaseMetaData metaData = beeLine.getDatabaseMetaData();
    // Get the databases
    try (ResultSet schemasResultSet = metaData.getSchemas()) {
      while (schemasResultSet.next()) {
        databases.add(schemasResultSet.getString("TABLE_SCHEM"));
      }
    }
    return databases;
  }

  private Set<String> getTables() throws SQLException {
    Set<String> tables = new HashSet<String>();

    DatabaseMetaData metaData = beeLine.getDatabaseMetaData();
    // Get the tables in the default database
    String[] types = new String[] {"TABLE"};
    try (ResultSet tablesResultSet = metaData.getTables(null, "default", "%", types)) {
      while (tablesResultSet.next()) {
        tables.add(tablesResultSet.getString("TABLE_NAME"));
      }
    }
    return tables;
  }

  private Set<String> getViews() throws SQLException {
    Set<String> views = new HashSet<String>();

    DatabaseMetaData metaData = beeLine.getDatabaseMetaData();
    // Get the tables in the default database
    String[] types = new String[] {"VIEW"};
    try (ResultSet tablesResultSet = metaData.getTables(null, "default", "%", types)) {
      while (tablesResultSet.next()) {
        views.add(tablesResultSet.getString("TABLE_NAME"));
      }
    }
    return views;
  }

  public void execute(String[] commands, File resultFile, Converter converter)
      throws Exception {
    beeLine.runCommands(
        new String[] {
          "!record " + resultFile.getAbsolutePath()
        });
    beeLine.setRecordOutputFile(new ConvertedOutputFile(beeLine.getRecordOutputFile(), converter));

    int lastSuccessfulCommand = beeLine.runCommands(commands);
    if (commands.length != lastSuccessfulCommand) {
      throw new SQLException("Error executing SQL command: " + commands[lastSuccessfulCommand]);
    }

    beeLine.runCommands(new String[] {"!record"});
  }

  private void beforeExecute(QFile qFile) throws Exception {
    String[] commands = TEST_FIRST_COMMANDS;

    String[] extraCommands;
    if (qFile.isUseSharedDatabase()) {
      // If we are using a shared database, then remove not known databases, tables, views.
      Set<String> dropCommands = getDatabases().stream()
          .filter(database -> !database.equals("default"))
          .map(database -> "DROP DATABASE `" + database + "` CASCADE;")
          .collect(Collectors.toSet());

      Set<String> srcTables = QTestUtil.getSrcTables();
      dropCommands.addAll(getTables().stream()
          .filter(table -> !srcTables.contains(table))
          .map(table -> "DROP TABLE `" + table + "` PURGE;")
          .collect(Collectors.toSet()));

      dropCommands.addAll(getViews().stream()
          .map(view -> "DROP VIEW `" + view + "`;")
          .collect(Collectors.toSet()));
      extraCommands = dropCommands.toArray(new String[]{});
    } else {
      // If we are using a test specific database, then we just drop the database, and recreate
      extraCommands = new String[] {
        "DROP DATABASE IF EXISTS `" + qFile.getDatabaseName() + "` CASCADE;",
        "CREATE DATABASE `" + qFile.getDatabaseName() + "`;",
        "USE `" + qFile.getDatabaseName() + "`;"
      };
    }
    commands = ArrayUtils.addAll(commands, extraCommands);
    commands = ArrayUtils.addAll(commands, TEST_SET_LOG_COMMANDS);
    execute(commands, qFile.getBeforeExecuteLogFile(), Converter.NONE);
    beeLine.setIsTestMode(true);
  }

  private void afterExecute(QFile qFile) throws Exception {
    beeLine.setIsTestMode(false);
    String[] commands = TEST_RESET_COMMANDS;

    if (!qFile.isUseSharedDatabase()) {
      // If we are using a test specific database, then we just drop the database
      String[] extraCommands = new String[] {
          "DROP DATABASE IF EXISTS `" + qFile.getDatabaseName() + "` CASCADE;"
      };
      commands = ArrayUtils.addAll(commands, extraCommands);
    }

    execute(commands, qFile.getAfterExecuteLogFile(), Converter.NONE);
  }

  public void execute(QFile qFile) throws Exception {
    beforeExecute(qFile);
    String[] commands = beeLine.getCommands(qFile.getInputFile());
    execute(qFile.filterCommands(commands), qFile.getRawOutputFile(), qFile.getConverter());
    afterExecute(qFile);
  }

  public void close() {
    if (beeLine != null) {
      beeLine.runCommands(new String[] {
        "!quit"
      });
    }
    if (beelineOutputStream != null) {
      beelineOutputStream.close();
    }
  }

  /**
   * Builder to generated QFileBeeLineClient objects. The after initializing the builder, it can be
   * used to create new clients without any parameters.
   */
  public static class QFileClientBuilder {
    private String username;
    private String password;
    private String jdbcUrl;
    private String jdbcDriver;

    public QFileClientBuilder() {
    }

    public QFileClientBuilder setUsername(String username) {
      this.username = username;
      return this;
    }

    public QFileClientBuilder setPassword(String password) {
      this.password = password;
      return this;
    }

    public QFileClientBuilder setJdbcUrl(String jdbcUrl) {
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public QFileClientBuilder setJdbcDriver(String jdbcDriver) {
      this.jdbcDriver = jdbcDriver;
      return this;
    }

    public QFileBeeLineClient getClient(File logFile) throws IOException {
      return new QFileBeeLineClient(jdbcUrl, jdbcDriver, username, password, logFile);
    }
  }
}
