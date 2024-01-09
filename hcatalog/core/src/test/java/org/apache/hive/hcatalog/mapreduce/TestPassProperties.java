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

package org.apache.hive.hcatalog.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Test;

public class TestPassProperties {
  private static final String TEST_DATA_DIR = System.getProperty("user.dir") +
      "/build/test/data/" + TestPassProperties.class.getCanonicalName();
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  private static IDriver driver;
  private static String[] input;
  private static HiveConf hiveConf;

  public void Initialize() throws Exception {
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));

    new File(TEST_WAREHOUSE_DIR).mkdirs();

    int numRows = 3;
    input = new String[numRows];
    for (int i = 0; i < numRows; i++) {
      String col1 = "a" + i;
      String col2 = "b" + i;
      input[i] = i + "," + col1 + "," + col2;
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
  }

  @Test
  public void testSequenceTableWriteReadMR() throws Exception {
    Initialize();
    String createTable = "CREATE TABLE bad_props_table(a0 int, a1 String, a2 String) STORED AS SEQUENCEFILE";
    driver.run("drop table bad_props_table");
    driver.run(createTable);

    boolean caughtException = false;
    try {
      Configuration conf = new Configuration();
      conf.set("hive.metastore.uris", "thrift://no.such.machine:10888");
      Job job = new Job(conf, "Write-hcat-seq-table");
      job.setJarByClass(TestPassProperties.class);

      job.setMapperClass(Map.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(DefaultHCatRecord.class);
      job.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.setInputPaths(job, INPUT_FILE_NAME);

      HCatOutputFormat.setOutput(job, OutputJobInfo.create(
          Warehouse.DEFAULT_DATABASE_NAME, "bad_props_table", null));
      job.setOutputFormatClass(HCatOutputFormat.class);
      HCatOutputFormat.setSchema(job, getSchema());
      job.setNumReduceTasks(0);
      assertTrue(job.waitForCompletion(true));
      new FileOutputCommitterContainer(job, null).cleanupJob(job);
    } catch (Exception e) {
      caughtException = true;
      assertTrue(((InvocationTargetException)e.getCause().getCause().getCause()).getTargetException().getMessage().contains(
          "Could not connect to meta store using any of the URIs provided"));
      assertTrue(e.getCause().getMessage().contains(
          "Unable to instantiate org.apache.hive.hcatalog.common.HiveClientCache$CacheableHiveMetaStoreClient"));
    }
    assertTrue(caughtException);
  }

  public static class Map extends Mapper<LongWritable, Text, NullWritable, DefaultHCatRecord> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] cols = value.toString().split(",");
      DefaultHCatRecord record = new DefaultHCatRecord(3);
      record.set(0, Integer.parseInt(cols[0]));
      record.set(1, cols[1]);
      record.set(2, cols[2]);
      context.write(NullWritable.get(), record);
    }
  }

  private HCatSchema getSchema() throws HCatException {
    HCatSchema schema = new HCatSchema(new ArrayList<HCatFieldSchema>());
    schema.append(new HCatFieldSchema("a0", HCatFieldSchema.Type.INT,
        ""));
    schema.append(new HCatFieldSchema("a1",
        HCatFieldSchema.Type.STRING, ""));
    schema.append(new HCatFieldSchema("a2",
        HCatFieldSchema.Type.STRING, ""));
    return schema;
  }


}
