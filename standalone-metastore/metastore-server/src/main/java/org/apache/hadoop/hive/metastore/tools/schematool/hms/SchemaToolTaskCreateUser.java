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
package org.apache.hadoop.hive.metastore.tools.schematool.hms;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

class SchemaToolTaskCreateUser extends MetaStoreTask {

  private static final String HIVE_USER = "hiveUser";
  private static final String HIVE_PASSWORD = "hivePassword";
  private static final String HIVE_DB = "hiveDb";
  private String hiveDb; // Hive database, for use when creating the user, not for connecting
  private String hivePasswd; // Hive password, for use when creating the user, not for connecting
  private String hiveUser; // Hive username, for use when creating the user, not for connecting

  @Override
  protected Set<String> usedCommandLineArguments() {
    return Sets.newHashSet(HIVE_USER, HIVE_PASSWORD, HIVE_DB);
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    if (commandLine.hasOption(HIVE_USER)) {
      hiveUser = commandLine.getOptionValue(HIVE_USER);
    }
    if (commandLine.hasOption(HIVE_PASSWORD)) {
      hivePasswd = commandLine.getOptionValue(HIVE_PASSWORD);
    }
    if (commandLine.hasOption(HIVE_DB)) {
      hiveDb = commandLine.getOptionValue(HIVE_DB);
    }

    testConnectionToMetastore(context);
    LOG.info("Starting user creation");

    SchemaInfo schemaInfo = context.getSchemaInfo();
    String scriptDir = schemaInfo.getMetaStoreScriptDir();
    String protoCreateFile = schemaInfo.getCreateUserScript();

    try {
      String dbType = commandLine.getDbType();
      File createFile = subUserAndPassword(scriptDir, protoCreateFile, dbType);
      LOG.info("Creation script " + createFile.getAbsolutePath());
      if (!commandLine.hasOption("dryRun")) {
        if ("oracle".equals(dbType)) oracleCreateUserHack(context, createFile);
        else {
          context.getScriptExecutor().execSql(createFile.getParent(), createFile.getName());
        }
        LOG.info("User creation completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("User creation FAILED!" +
          " Metastore unusable !!", e);
    }

  }

  private File subUserAndPassword(String parent, String filename, String dbType) throws IOException {
    File createFile = File.createTempFile("create-hive-user-" + dbType, ".sql");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(createFile))) {
      File proto = new File(parent, filename);
      try (BufferedReader reader = new BufferedReader(new FileReader(proto))) {
        reader.lines()
            .map(s -> s.replace("_REPLACE_WITH_USER_", hiveUser)
                .replace("_REPLACE_WITH_PASSWD_", hivePasswd)
                .replace("_REPLACE_WITH_DB_", hiveDb))
            .forEach(s -> {
              try {
                writer.write(s);
                writer.newLine();
              } catch (IOException e) {
                throw new RuntimeException("Unable to write to tmp file ", e);
              }
            });
      }
    }
    return createFile;
  }

  private void oracleCreateUserHack(TaskContext context, File createFile) throws HiveMetaException {
    LOG.debug("Found oracle, hacking our way through it rather than using SqlLine");
    try (BufferedReader reader = new BufferedReader(new FileReader(createFile))) {
      try (Connection conn = context.getConnectionToMetastore(false)) {
        try (Statement stmt = conn.createStatement()) {
          reader.lines()
              .forEach(s -> {
                assert s.charAt(s.length() - 1) == ';';
                try {
                  stmt.execute(s.substring(0, s.length() - 1));
                } catch (SQLException e) {
                  LOG.error("statement <" + s.substring(0, s.length() - 2) + "> failed", e);
                  throw new RuntimeException(e);
                }
              });
        }
      }
    } catch (IOException e) {
      LOG.error("Caught IOException trying to read modified create user script " +
          createFile.getAbsolutePath(), e);
      throw new HiveMetaException(e);
    } catch (HiveMetaException e) {
      LOG.error("Failed to connect to RDBMS", e);
      throw e;
    } catch (SQLException e) {
      LOG.error("Got SQLException", e);
    }
  }

}
