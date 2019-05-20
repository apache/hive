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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestSystemProperties;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QTestDatasetHandler {
  private static final Logger LOG = LoggerFactory.getLogger("QTestDatasetHandler");

  private File datasetDir;
  private QTestUtil qt;
  private static Set<String> srcTables;

  public QTestDatasetHandler(QTestUtil qTestUtil, HiveConf conf) {
    // Use path relative to dataDir directory if it is not specified
    String dataDir = getDataDir(conf);

    datasetDir = conf.get("test.data.set.files") == null ? new File(dataDir + "/datasets")
      : new File(conf.get("test.data.set.files"));
    this.qt = qTestUtil;
  }

  public String getDataDir(HiveConf conf) {
    String dataDir = conf.get("test.data.files");
    // Use the current directory if it is not specified
    if (dataDir == null) {
      dataDir = new File(".").getAbsolutePath() + "/data/files";
    }

    return dataDir;
  }

  public void initDataSetForTest(File file, CliDriver cliDriver) throws Exception {
    synchronized (QTestUtil.class) {
      DatasetParser parser = new DatasetParser();
      parser.parse(file);

      DatasetCollection datasets = parser.getDatasets();

      Set<String> missingDatasets = datasets.getTables();
      missingDatasets.removeAll(getSrcTables());
      if (missingDatasets.isEmpty()) {
        return;
      }
      qt.newSession(true);
      for (String table : missingDatasets) {
        if (initDataset(table, cliDriver)) {
          addSrcTable(table);
        }
      }
      qt.newSession(true);
    }
  }

  public boolean initDataset(String table, CliDriver cliDriver) throws Exception {
    File tableFile = new File(new File(datasetDir, table), Dataset.INIT_FILE_NAME);
    String commands = null;
    try {
      commands = FileUtils.readFileToString(tableFile);
    } catch (IOException e) {
      throw new RuntimeException(String.format("dataset file not found %s", tableFile), e);
    }

    CommandProcessorResponse result = cliDriver.processLine(commands);
    LOG.info("Result from cliDrriver.processLine in initFromDatasets=" + result);
    if (result.getResponseCode() != 0) {
      Assert.fail("Failed during initFromDatasets processLine with code=" + result);
    }

    return true;
  }

  public static Set<String> getSrcTables() {
    if (srcTables == null) {
      initSrcTables();
    }
    return srcTables;
  }

  public static void addSrcTable(String table) {
    getSrcTables().add(table);
    storeSrcTables();
  }

  public static Set<String> initSrcTables() {
    if (srcTables == null) {
      initSrcTablesFromSystemProperty();
      storeSrcTables();
    }

    return srcTables;
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
}
