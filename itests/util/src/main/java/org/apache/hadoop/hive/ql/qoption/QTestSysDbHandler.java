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

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.ql.QTestUtil;
import org.apache.hive.beeline.schematool.HiveSchemaTool;
import org.apache.hive.testutils.HiveTestEnvSetup;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * QTest sysdb directive handler
 *
 * Loads the sysdb schema prior to running the test.
 *
 * Example:
 * --! qt:sysdb
 * 
 */
public class QTestSysDbHandler implements QTestOptionHandler {

  private boolean enabled;

  @Override
  public void processArguments(String arguments) {
    enabled = true;
  }

  @Override
  public void beforeTest(QTestUtil qt) throws Exception {
    if (enabled) {
      // org.apache.hadoop.hive.metastore.utils.SchemaToolTestUtil.executeCommand(java.lang.String, org.apache.hadoop.conf.Configuration, java.lang.String[])
      // could handle the SYS schema upgrade, but it uses Beeline while the Qtest framework uses the CliDriver, and the 
      // two are not compatible. As a result of the above, Hive schema creation needs to be done 'manually' using CliDriver.
      String scriptOrder = HiveTestEnvSetup.HIVE_ROOT + "/metastore/scripts/upgrade/hive/upgrade.order.hive";
      String filePath = null;
      try (FileReader fr = new FileReader(scriptOrder); BufferedReader bfReader = new BufferedReader(fr)) {
        String version, latestVersion = "Unknown";
        while ((version = bfReader.readLine()) != null) {
          latestVersion = version.split("\\-to\\-")[1]; 
          filePath = HiveTestEnvSetup.HIVE_ROOT + "/standalone-metastore/metastore-server/src/main/sql/hive/upgrade-" + version + ".hive.sql";
          qt.getCliDriver().processLine("source " + filePath);
        }
        qt.getCliDriver().processLine("USE SYS;");
        qt.getCliDriver().processLine(String.format(HiveSchemaTool.VERSION_SCRIPT + ";", latestVersion, latestVersion));
        qt.getCliDriver().processLine("USE INFORMATION_SCHEMA;");
        qt.getCliDriver().processLine(String.format(HiveSchemaTool.VERSION_SCRIPT + ";", latestVersion, latestVersion));
      } catch (FileNotFoundException e) {
        throw new HiveMetaException("File " + filePath + " not found ", e);
      } catch (IOException e) {
        throw new HiveMetaException("Error reading " + filePath, e);
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new HiveMetaException("Invalid version range found in upgrade.order.hive file!", e);
      }
      qt.getCliDriver().processLine("use default");
    }
  }

  @Override
  public void afterTest(QTestUtil qt) throws Exception {
    enabled = false;
  }

}
