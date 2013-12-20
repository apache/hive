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

package org.apache.hcatalog.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.TestSequenceFileReadWrite} instead
 */
public class TestSequenceFileReadWrite {

  private File dataDir;
  private String warehouseDir;
  private String inputFileName;
  private Driver driver;
  private PigServer server;
  private String[] input;
  private HiveConf hiveConf;

  @Before
  public void setup() throws Exception {
    dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator +
        TestSequenceFileReadWrite.class.getCanonicalName() + "-" + System.currentTimeMillis());
    hiveConf = new HiveConf(this.getClass());
    warehouseDir = HCatUtil.makePathASafeFileName(dataDir + File.separator + "warehouse");
    inputFileName = HCatUtil.makePathASafeFileName(dataDir + File.separator + "input.data");
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir);
    driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));

    if(!(new File(warehouseDir).mkdirs())) {
      throw new RuntimeException("Could not create " + warehouseDir);
    }

    int numRows = 3;
    input = new String[numRows];
    for (int i = 0; i < numRows; i++) {
      String col1 = "a" + i;
      String col2 = "b" + i;
      input[i] = i + "," + col1 + "," + col2;
    }
    HcatTestUtils.createTestDataFile(inputFileName, input);
    server = new PigServer(ExecType.LOCAL);
  }

  @After
  public void teardown() throws IOException {
    if(dataDir != null) {
      FileUtils.deleteDirectory(dataDir);
    }
  }

  @Test
  public void testSequenceTableWriteRead() throws Exception {
    String createTable = "CREATE TABLE demo_table(a0 int, a1 String, a2 String) STORED AS SEQUENCEFILE";
    driver.run("drop table demo_table");
    int retCode1 = driver.run(createTable).getResponseCode();
    assertTrue(retCode1 == 0);

    server.setBatchOn();
    server.registerQuery("A = load '"
        + inputFileName
        + "' using PigStorage(',') as (a0:int,a1:chararray,a2:chararray);");
    server.registerQuery("store A into 'demo_table' using org.apache.hcatalog.pig.HCatStorer();");
    server.executeBatch();

    server.registerQuery("B = load 'demo_table' using org.apache.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> XIter = server.openIterator("B");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(3, t.size());
      assertEquals(t.get(0).toString(), "" + numTuplesRead);
      assertEquals(t.get(1).toString(), "a" + numTuplesRead);
      assertEquals(t.get(2).toString(), "b" + numTuplesRead);
      numTuplesRead++;
    }
    assertEquals(input.length, numTuplesRead);
  }

  @Test
  public void testTextTableWriteRead() throws Exception {
    String createTable = "CREATE TABLE demo_table_1(a0 int, a1 String, a2 String) STORED AS TEXTFILE";
    driver.run("drop table demo_table_1");
    int retCode1 = driver.run(createTable).getResponseCode();
    assertTrue(retCode1 == 0);

    server.setBatchOn();
    server.registerQuery("A = load '"
        + inputFileName
        + "' using PigStorage(',') as (a0:int,a1:chararray,a2:chararray);");
    server.registerQuery("store A into 'demo_table_1' using org.apache.hcatalog.pig.HCatStorer();");
    server.executeBatch();

    server.registerQuery("B = load 'demo_table_1' using org.apache.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> XIter = server.openIterator("B");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(3, t.size());
      assertEquals(t.get(0).toString(), "" + numTuplesRead);
      assertEquals(t.get(1).toString(), "a" + numTuplesRead);
      assertEquals(t.get(2).toString(), "b" + numTuplesRead);
      numTuplesRead++;
    }
    assertEquals(input.length, numTuplesRead);
  }

  @Test
  public void testSequenceTableWriteReadMR() throws Exception {
    String createTable = "CREATE TABLE demo_table_2(a0 int, a1 String, a2 String) STORED AS SEQUENCEFILE";
    driver.run("drop table demo_table_2");
    int retCode1 = driver.run(createTable).getResponseCode();
    assertTrue(retCode1 == 0);

    Configuration conf = new Configuration();
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(hiveConf.getAllProperties()));
    Job job = new Job(conf, "Write-hcat-seq-table");
    job.setJarByClass(TestSequenceFileReadWrite.class);

    job.setMapperClass(Map.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, inputFileName);

    HCatOutputFormat.setOutput(job, OutputJobInfo.create(
        MetaStoreUtils.DEFAULT_DATABASE_NAME, "demo_table_2", null));
    job.setOutputFormatClass(HCatOutputFormat.class);
    HCatOutputFormat.setSchema(job, getSchema());
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
    if (!HCatUtil.isHadoop23()) {
      new FileOutputCommitterContainer(job, null).commitJob(job);
    }
    assertTrue(job.isSuccessful());

    server.setBatchOn();
    server.registerQuery("C = load 'default.demo_table_2' using org.apache.hcatalog.pig.HCatLoader();");
    server.executeBatch();
    Iterator<Tuple> XIter = server.openIterator("C");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(3, t.size());
      assertEquals(t.get(0).toString(), "" + numTuplesRead);
      assertEquals(t.get(1).toString(), "a" + numTuplesRead);
      assertEquals(t.get(2).toString(), "b" + numTuplesRead);
      numTuplesRead++;
    }
    assertEquals(input.length, numTuplesRead);
  }

  @Test
  public void testTextTableWriteReadMR() throws Exception {
    String createTable = "CREATE TABLE demo_table_3(a0 int, a1 String, a2 String) STORED AS TEXTFILE";
    driver.run("drop table demo_table_3");
    int retCode1 = driver.run(createTable).getResponseCode();
    assertTrue(retCode1 == 0);

    Configuration conf = new Configuration();
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(hiveConf.getAllProperties()));
    Job job = new Job(conf, "Write-hcat-text-table");
    job.setJarByClass(TestSequenceFileReadWrite.class);

    job.setMapperClass(Map.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setNumReduceTasks(0);
    TextInputFormat.setInputPaths(job, inputFileName);

    HCatOutputFormat.setOutput(job, OutputJobInfo.create(
        MetaStoreUtils.DEFAULT_DATABASE_NAME, "demo_table_3", null));
    job.setOutputFormatClass(HCatOutputFormat.class);
    HCatOutputFormat.setSchema(job, getSchema());
    assertTrue(job.waitForCompletion(true));
    if (!HCatUtil.isHadoop23()) {
      new FileOutputCommitterContainer(job, null).commitJob(job);
    }
    assertTrue(job.isSuccessful());

    server.setBatchOn();
    server.registerQuery("D = load 'default.demo_table_3' using org.apache.hcatalog.pig.HCatLoader();");
    server.executeBatch();
    Iterator<Tuple> XIter = server.openIterator("D");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(3, t.size());
      assertEquals(t.get(0).toString(), "" + numTuplesRead);
      assertEquals(t.get(1).toString(), "a" + numTuplesRead);
      assertEquals(t.get(2).toString(), "b" + numTuplesRead);
      numTuplesRead++;
    }
    assertEquals(input.length, numTuplesRead);
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
