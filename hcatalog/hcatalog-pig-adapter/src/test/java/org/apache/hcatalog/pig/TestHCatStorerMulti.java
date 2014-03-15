/**
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
package org.apache.hcatalog.pig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.data.Pair;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.TestHCatStorerMulti} instead
 */
public class TestHCatStorerMulti extends TestCase {
  private static final String TEST_DATA_DIR = org.apache.hive.hcatalog.pig.TestHCatStorerMulti.TEST_DATA_DIR;
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  private static Driver driver;

  private static Map<Integer, Pair<Integer, String>> basicInputData;

  protected String storageFormat() {
    return "RCFILE tblproperties('hcat.isd'='org.apache.hcatalog.rcfile.RCFileInputDriver'," +
      "'hcat.osd'='org.apache.hcatalog.rcfile.RCFileOutputDriver')";
  }

  private void dropTable(String tablename) throws IOException, CommandNeedRetryException {
    driver.run("drop table " + tablename);
  }

  private void createTable(String tablename, String schema, String partitionedBy) throws IOException, CommandNeedRetryException {
    String createTable;
    createTable = "create table " + tablename + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      createTable = createTable + "partitioned by (" + partitionedBy + ") ";
    }
    createTable = createTable + "stored as " + storageFormat();
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table. [" + createTable + "], return code from hive driver : [" + retCode + "]");
    }
  }

  private void createTable(String tablename, String schema) throws IOException, CommandNeedRetryException {
    createTable(tablename, schema, null);
  }

  @Override
  protected void setUp() throws Exception {
    if (driver == null) {
      HiveConf hiveConf = new HiveConf(this.getClass());
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
      driver = new Driver(hiveConf);
      SessionState.start(new CliSessionState(hiveConf));
    }

    cleanup();
  }

  @Override
  protected void tearDown() throws Exception {
    cleanup();
  }

  public void testStoreBasicTable() throws Exception {


    createTable(BASIC_TABLE, "a int, b string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatStorer();");

    server.executeBatch();

    driver.run("select * from " + BASIC_TABLE);
    ArrayList<String> unpartitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(unpartitionedTableValuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), unpartitionedTableValuesReadFromHiveDriver.size());
  }

  public void testStorePartitionedTable() throws Exception {
    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=1');");

    server.executeBatch();

    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> partitionedTableValuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(partitionedTableValuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), partitionedTableValuesReadFromHiveDriver.size());
  }

  public void testStoreTableMulti() throws Exception {


    createTable(BASIC_TABLE, "a int, b string");
    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");

    populateBasicFile();

    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatStorer();");

    server.registerQuery("B2 = filter A by a < 2;");
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=0');");
    server.registerQuery("C2 = filter A by a >= 2;");
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=1');");

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

  private void cleanup() throws IOException, CommandNeedRetryException {
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    new File(TEST_WAREHOUSE_DIR).mkdirs();

    dropTable(BASIC_TABLE);
    dropTable(PARTITIONED_TABLE);
  }
}
