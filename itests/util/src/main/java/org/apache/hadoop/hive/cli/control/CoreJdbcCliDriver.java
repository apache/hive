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
package org.apache.hadoop.hive.cli.control;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.cli.control.CoreCliDriver;
import org.apache.hadoop.hive.cli.control.AbstractCliConfig;
import org.apache.hadoop.hive.ql.externalDB.AbstractExternalDB;
import org.apache.hadoop.hive.ql.qoption.QTestDatabaseHandler;
import org.apache.hadoop.hive.ql.QTestArguments;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CoreJdbcCliDriver extends CoreCliDriver {
  private AbstractExternalDB externalDB;
  private static final Logger LOG = LoggerFactory.getLogger(CoreJdbcCliDriver.class);
  private boolean externalTablesCreated = false;
  
  public CoreJdbcCliDriver(AbstractCliConfig testCliConfig) {
    super(testCliConfig);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    
    if (cliConfig.getJdbcInitScript() != null) {
      LOG.info("Launching docker container, running jdbc init script...");
      java.nio.file.Path scriptFile = Paths.get(
          QTestUtil.getScriptsDir(getQt().getConf()) + File.separator + cliConfig.getJdbcInitScript()
      );
      if (Files.notExists(scriptFile)) {
        LOG.info("No jdbc init script detected. Skipping");
        return;
      }
      externalDB = QTestDatabaseHandler.DatabaseType.valueOf("POSTGRES").create();
      externalDB.launchDockerContainer();
      externalDB.execute(scriptFile.toString());
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    if (!externalTablesCreated && cliConfig.getExternalTablesForJdbcInitScript() != null) {
      LOG.info("Running init script for external tables...");
      File scriptFile = new File(
          QTestUtil.getScriptsDir(getQt().getConf()) + File.separator + 
              cliConfig.getExternalTablesForJdbcInitScript()
      );
      if (!scriptFile.isFile()) {
        LOG.info("No init script for external tables detected. Skipping");
        return;
      }
      String initCommands = FileUtils.readFileToString(scriptFile);
      getQt().getCliDriver().processLine(initCommands);
      externalTablesCreated = true;
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    getQt().clearPostTestEffects();
  }

  @Override
  @AfterClass
  public void shutdown() throws Exception {
    LOG.info("Cleaning up...");
    super.tearDown();
    super.shutdown();
    if (externalDB != null) {
      LOG.info("Cleaning up docker...");
      externalDB.cleanupDockerContainer();
    }
  }
}
