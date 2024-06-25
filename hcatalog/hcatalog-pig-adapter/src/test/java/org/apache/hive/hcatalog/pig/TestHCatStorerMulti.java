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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.pig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.StorageFormats;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.Pair;
import org.apache.hive.hcatalog.mapreduce.HCatBaseTest;
import org.apache.pig.PigServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class TestHCatStorerMulti {
  public static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(
      System.getProperty("user.dir") + "/build/test/data/" +
          TestHCatStorerMulti.class.getCanonicalName() + "-" + System.currentTimeMillis());
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  private static IDriver driver;

  private static Map<Integer, Pair<Integer, String>> basicInputData;

  private static final Map<String, Set<String>> DISABLED_STORAGE_FORMATS =
      new HashMap<String, Set<String>>();

  private final String storageFormat;

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return StorageFormats.names();
  }

  public TestHCatStorerMulti(String storageFormat) {
    this.storageFormat = storageFormat;
  }

  private void dropTable(String tablename) throws Exception {
    driver.run("drop table " + tablename);
  }

  private void createTable(String tablename, String schema, String partitionedBy) throws Exception {
    AbstractHCatLoaderTest.createTableDefaultDB(tablename, schema, partitionedBy, driver, storageFormat);
  }

  private void createTable(String tablename, String schema) throws Exception {
    createTable(tablename, schema, null);
  }

  @Before
  public void setUp() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    if (driver == null) {
      HiveConf hiveConf = new HiveConf(this.getClass());
      //TODO: HIVE-27998: hcatalog tests on Tez
      hiveConf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
      hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, TEST_WAREHOUSE_DIR);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
      hiveConf
      .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
      driver = DriverFactory.newDriver(hiveConf);
      SessionState.start(new CliSessionState(hiveConf));
    }

    cleanup();
  }

  @After
  public void tearDown() throws Exception {
    cleanup();
  }

  @Test
  public void testStoreBasicTable() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    createTable(BASIC_TABLE, "a int, b string");

    populateBasicFile();

    PigServer server = HCatBaseTest.createPigServer(false);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();");

    server.executeBatch();

    driver.run("select * from " + BASIC_TABLE);
    ArrayList<String> unpartitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(unpartitionedTableValuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), unpartitionedTableValuesReadFromHiveDriver.size());
  }

  @Test
  public void testStorePartitionedTable() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");

    populateBasicFile();

    PigServer server = HCatBaseTest.createPigServer(false);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=1');");

    server.executeBatch();

    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> partitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(partitionedTableValuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), partitionedTableValuesReadFromHiveDriver.size());
  }

  @Test
  public void testStoreTableMulti() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    createTable(BASIC_TABLE, "a int, b string");
    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");

    populateBasicFile();

    PigServer server = HCatBaseTest.createPigServer(false);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=1');");

    server.executeBatch();

    driver.run("select * from " + BASIC_TABLE);
    ArrayList<String> unpartitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(unpartitionedTableValuesReadFromHiveDriver);
    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> partitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(partitionedTableValuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), unpartitionedTableValuesReadFromHiveDriver.size());
    assertEquals(basicInputData.size(), partitionedTableValuesReadFromHiveDriver.size());
  }

  private void populateBasicFile() throws IOException {
    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    basicInputData = new HashMap<Integer, Pair<Integer, String>>();
    int k = 0;
    File file = new File(INPUT_FILE_NAME);
    file.deleteOnExit();
    FileWriter writer = new FileWriter(file);
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input[k] = si + "\t" + sj;
        basicInputData.put(k, new Pair<Integer, String>(i, sj));
        writer.write(input[k] + "\n");
        k++;
      }
    }
    writer.close();
  }

  private void cleanup() throws Exception {
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    new File(TEST_WAREHOUSE_DIR).mkdirs();

    dropTable(BASIC_TABLE);
    dropTable(PARTITIONED_TABLE);
  }
}
