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
package org.apache.hadoop.hive.metastore.tools.schematool;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemaToolTaskCreateUser extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskCreateUser.class);

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {

  }

  @Override
  void execute() throws HiveMetaException {
    schemaTool.testConnectionToMetastore();
    System.out.println("Starting user creation");

    String scriptDir = schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir();
    String protoCreateFile = schemaTool.getMetaStoreSchemaInfo().getCreateUserScript();

    try {
      File createFile = subUserAndPassword(scriptDir, protoCreateFile);
      System.out.println("Creation script " + createFile.getAbsolutePath());
      if (!schemaTool.isDryRun()) {
        if ("oracle".equals(schemaTool.getDbType())) oracleCreateUserHack(createFile);
        else schemaTool.execSql(createFile.getParent(), createFile.getName());
        System.out.println("User creation completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("User creation FAILED!" +
          " Metastore unusable !!", e);
    }

  }

  private File subUserAndPassword(String parent, String filename) throws IOException {
    File createFile = File.createTempFile("create-hive-user-" + schemaTool.getDbType(), ".sql");
    BufferedWriter writer = new BufferedWriter(new FileWriter(createFile));
    File proto = new File(parent, filename);
    BufferedReader reader = new BufferedReader(new FileReader(proto));
    reader.lines()
        .map(s -> s.replace("_REPLACE_WITH_USER_", schemaTool.getHiveUser())
            .replace("_REPLACE_WITH_PASSWD_", schemaTool.getHivePasswd())
            .replace("_REPLACE_WITH_DB_", schemaTool.getHiveDb()))
        .forEach(s -> {
          try {
            writer.write(s);
            writer.newLine();
          } catch (IOException e) {
            throw new RuntimeException("Unable to write to tmp file ", e);
          }
        });
    reader.close();
    writer.close();
    return createFile;
  }

  private void oracleCreateUserHack(File createFile) throws HiveMetaException {
    LOG.debug("Found oracle, hacking our way through it rather than using SqlLine");
    try (BufferedReader reader = new BufferedReader(new FileReader(createFile))) {
      try (Connection conn = schemaTool.getConnectionToMetastore(false)) {
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
