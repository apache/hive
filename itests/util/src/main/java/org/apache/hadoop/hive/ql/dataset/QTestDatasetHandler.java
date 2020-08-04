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

package org.apache.hadoop.hive.ql.dataset;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestSystemProperties;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.qoption.QTestOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Datasets are provided by this handler.
 *
 * An invocation of:
 *
 * <pre>
 * --! qt:dataset:sample
 * --! qt:dataset:sample:ONLY
 * </pre>
 *
 * will make sure that the dataset named sample is loaded prior to executing the test.
 */
public class QTestDatasetHandler implements QTestOptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger("QTestDatasetHandler");

  private final File datasetDir;
  /**
   * All tables already loaded in the database.
   */
  private static Set<String> srcTables;
  /**
   * Tables mentioned explicitly inside a single QFile and not yet loaded to the database.
   */
  private final Set<String> explicitTables = new HashSet<>();
  /**
   * Indicates if implicit tables (srcTables MINUS explicitTables) need to be unloaded.
   */
  private boolean unloadImplicitTables = false;

  public QTestDatasetHandler(HiveConf conf) {
    // Use path relative to dataDir directory if it is not specified
    String dataDir = getDataDir(conf);

    datasetDir = conf.get("test.data.set.files") == null ? new File(dataDir + "/datasets")
      : new File(conf.get("test.data.set.files"));
  }

  public String getDataDir(HiveConf conf) {
    String dataDir = conf.get("test.data.files");
    // Use the current directory if it is not specified
    if (dataDir == null) {
      dataDir = new File(".").getAbsolutePath() + "/data/files";
    }

    return dataDir;
  }

  public void initDataset(String table, CliDriver cliDriver) {
    File tableFile = new File(new File(datasetDir, table), Dataset.INIT_FILE_NAME);
    String commands;
    try {
      commands = FileUtils.readFileToString(tableFile);
    } catch (IOException e) {
      throw new RuntimeException(String.format("dataset file not found %s", tableFile), e);
    }

    try {
      CommandProcessorResponse result = cliDriver.processLine(commands);
      LOG.info("Result from cliDrriver.processLine in initDataset=" + result);
    } catch (CommandProcessorException e) {
      throw new RuntimeException("Failed while loading table " + table, e);
    }
    // Add the table in sources if it is loaded successfully
    addSrcTable(table);
  }

  private void unloadDataset(String table, CliDriver cliDriver) {
    try {
      // Remove table from sources otherwise the following command will fail due to EnforceReadOnlyTables.
      removeSrcTable(table);
      CommandProcessorResponse result = cliDriver.processLine("drop table " + table);
      LOG.info("Result from cliDrriver.processLine in unloadDataset=" + result);
    } catch (CommandProcessorException e) {
      // If the unloading fails for any reason then add again the table to sources since it is still there.
      addSrcTable(table);
      throw new RuntimeException("Failed while unloading table " + table, e);
    }
  }

  public static Set<String> getSrcTables() {
    if (srcTables == null) {
      initSrcTablesFromSystemProperty();
      storeSrcTables();
    }
    return srcTables;
  }

  private static void addSrcTable(String table) {
    getSrcTables().add(table);
    storeSrcTables();
  }

  private void removeSrcTable(String table) {
    srcTables.remove(table);
    storeSrcTables();
  }

  public static boolean isSourceTable(String name) {
    return getSrcTables().contains(name);
  }

  private static void storeSrcTables() {
    QTestSystemProperties.setSrcTables(srcTables);
  }

  private static void initSrcTablesFromSystemProperty() {
    srcTables = new HashSet<String>();
    // FIXME: moved default value to here...for now
    // i think this features is never really used from the command line
    for (String srcTable : QTestSystemProperties.getSrcTables()) {
      srcTable = srcTable.trim();
      if (!srcTable.isEmpty()) {
        srcTables.add(srcTable);
      }
    }
  }

  @Override
  public void processArguments(String arguments) {
    String[] args = arguments.split(":");
    Set<String> tableNames = getTableNames(args[0]);
    synchronized (QTestUtil.class) {
      if (args.length > 1) {
        if (args.length > 2 || !args[1].equalsIgnoreCase("ONLY")) {
          throw new RuntimeException("unknown option: " + args[1]);
        }
        unloadImplicitTables = true;
      }
      explicitTables.addAll(tableNames);
    }
  }

  private Set<String> getTableNames(String arguments) {
    Set<String> ret = new HashSet<String>();
    String[] tables = arguments.split(",");
    for (String string : tables) {
      string = string.trim();
      if (string.length() == 0) {
        continue;
      }
      ret.add(string);
    }
    return ret;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (explicitTables.isEmpty() && !unloadImplicitTables) {
      return;
    }
    // Concurrency note: Determining the tables to load/unload
    // and performing the respective operation should happen
    // under the same lock in an atomic way otherwise we may
    // suffer from check-then-act race conditions.
    synchronized (QTestUtil.class) {
      qt.newSession(true);
      try {
        for (String table : tablesToLoad()) {
          initDataset(table, qt.getCliDriver());
        }
        for (String table : tablesToUnload()) {
          unloadDataset(table, qt.getCliDriver());
        }
      } finally {
        explicitTables.clear();
        unloadImplicitTables = false;
        qt.newSession(true);
      }
    }
  }

  private Set<String> tablesToLoad() {
    if (explicitTables.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> tables = new HashSet<>(explicitTables);
    tables.removeAll(getSrcTables());
    return tables;
  }

  private Set<String> tablesToUnload() {
    if (unloadImplicitTables) {
      return Collections.emptySet();
    }
    Set<String> tables = new HashSet<>(getSrcTables());
    tables.removeAll(explicitTables);
    return tables;
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
  }

  public DatasetCollection getDatasets() {
    return new DatasetCollection(explicitTables);
  }

}
