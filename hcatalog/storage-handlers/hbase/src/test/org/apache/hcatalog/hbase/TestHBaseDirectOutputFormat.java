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

package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.FamilyRevision;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerConfiguration;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Test HBaseDirectOUtputFormat and HBaseHCatStorageHandler using a MiniCluster
 */
public class TestHBaseDirectOutputFormat extends SkeletonHBaseTest {

  private final HiveConf allConf;
  private final HCatDriver hcatDriver;

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  public TestHBaseDirectOutputFormat() {
    allConf = getHiveConf();
    allConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
      HCatSemanticAnalyzer.class.getName());
    allConf.set(HiveConf.ConfVars.HADOOPFS.varname, getFileSystem().getUri().toString());
    allConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new Path(getTestDir(), "warehouse").toString());

    //Add hbase properties
    for (Map.Entry<String, String> el : getHbaseConf())
      allConf.set(el.getKey(), el.getValue());
    for (Map.Entry<String, String> el : getJobConf())
      allConf.set(el.getKey(), el.getValue());
    HBaseConfiguration.merge(
      allConf,
      RevisionManagerConfiguration.create());
    SessionState.start(new CliSessionState(allConf));
    hcatDriver = new HCatDriver();
  }

  @Test
  public void directOutputFormatTest() throws IOException, ClassNotFoundException, InterruptedException {
    String testName = "directOutputFormatTest";
    Path methodTestDir = new Path(getTestDir(), testName);

    String tableName = newTableName(testName).toLowerCase();
    String familyName = "my_family";
    byte[] familyNameBytes = Bytes.toBytes(familyName);

    //include hbase config in conf file
    Configuration conf = new Configuration(allConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));

    //create table
    createTable(tableName, new String[]{familyName});

    String data[] = {"1,english:ONE,spanish:UNO",
      "2,english:ONE,spanish:DOS",
      "3,english:ONE,spanish:TRES"};


    // input/output settings
    Path inputPath = new Path(methodTestDir, "mr_input");
    getFileSystem().mkdirs(inputPath);
    FSDataOutputStream os = getFileSystem().create(new Path(inputPath, "inputFile.txt"));
    for (String line : data)
      os.write(Bytes.toBytes(line + "\n"));
    os.close();

    //create job
    JobConf job = new JobConf(conf);
    job.setJobName(testName);
    job.setWorkingDirectory(new Path(methodTestDir, "mr_work"));
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapWrite.class);

    job.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
    org.apache.hadoop.mapred.TextInputFormat.setInputPaths(job, inputPath);

    job.setOutputFormat(HBaseDirectOutputFormat.class);
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    job.set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, tableName);

    //manually create transaction
    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(conf);
    try {
      OutputJobInfo outputJobInfo = OutputJobInfo.create("default", tableName, null);
      Transaction txn = rm.beginWriteTransaction(tableName, Arrays.asList(familyName));
      outputJobInfo.getProperties().setProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY,
        HCatUtil.serialize(txn));
      job.set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
        HCatUtil.serialize(outputJobInfo));
    } finally {
      rm.close();
    }

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(HCatRecord.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(HCatRecord.class);
    job.setNumReduceTasks(0);

    RunningJob runJob = JobClient.runJob(job);
    runJob.waitForCompletion();
    assertTrue(runJob.isSuccessful());

    //verify
    HTable table = new HTable(conf, tableName);
    Scan scan = new Scan();
    scan.addFamily(familyNameBytes);
    ResultScanner scanner = table.getScanner(scan);
    int index = 0;
    for (Result result : scanner) {
      String vals[] = data[index].toString().split(",");
      for (int i = 1; i < vals.length; i++) {
        String pair[] = vals[i].split(":");
        assertTrue(result.containsColumn(familyNameBytes, Bytes.toBytes(pair[0])));
        assertEquals(pair[1], Bytes.toString(result.getValue(familyNameBytes, Bytes.toBytes(pair[0]))));
      }
      index++;
    }
    assertEquals(data.length, index);
  }

  @Test
  public void directHCatOutputFormatTest() throws Exception {
    String testName = "directHCatOutputFormatTest";
    Path methodTestDir = new Path(getTestDir(), testName);

    String databaseName = testName;
    String dbDir = new Path(methodTestDir, "DB_" + testName).toString();
    String tableName = newTableName(testName);
    String familyName = "my_family";
    byte[] familyNameBytes = Bytes.toBytes(familyName);
    //Table name will be lower case unless specified by hbase.table.name property
    String hbaseTableName = (databaseName + "." + tableName).toLowerCase();

    //include hbase config in conf file
    Configuration conf = new Configuration(allConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));


    String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '" + dbDir + "'";
    String tableQuery = "CREATE TABLE " + databaseName + "." + tableName +
      "(key int, english string, spanish string) STORED BY " +
      "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'" +
      "TBLPROPERTIES (" +
      "'hbase.columns.mapping'=':key," + familyName + ":english," + familyName + ":spanish')";

    assertEquals(0, hcatDriver.run(dbquery).getResponseCode());
    assertEquals(0, hcatDriver.run(tableQuery).getResponseCode());

    String data[] = {"1,english:ONE,spanish:UNO",
      "2,english:ONE,spanish:DOS",
      "3,english:ONE,spanish:TRES"};

    // input/output settings
    Path inputPath = new Path(methodTestDir, "mr_input");
    getFileSystem().mkdirs(inputPath);
    //create multiple files so we can test with multiple mappers
    for (int i = 0; i < data.length; i++) {
      FSDataOutputStream os = getFileSystem().create(new Path(inputPath, "inputFile" + i + ".txt"));
      os.write(Bytes.toBytes(data[i] + "\n"));
      os.close();
    }

    //create job
    Path workingDir = new Path(methodTestDir, "mr_work");
    OutputJobInfo outputJobInfo = OutputJobInfo.create(databaseName,
      tableName, null);
    Job job = configureJob(testName, conf, workingDir, MapHCatWrite.class,
      outputJobInfo, inputPath);
    assertTrue(job.waitForCompletion(true));

    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(conf);
    try {
      TableSnapshot snapshot = rm.createSnapshot(hbaseTableName);
      for (String el : snapshot.getColumnFamilies()) {
        assertEquals(1, snapshot.getRevision(el));
      }
    } finally {
      rm.close();
    }

    //verify
    HTable table = new HTable(conf, hbaseTableName);
    Scan scan = new Scan();
    scan.addFamily(familyNameBytes);
    ResultScanner scanner = table.getScanner(scan);
    int index = 0;
    for (Result result : scanner) {
      String vals[] = data[index].toString().split(",");
      for (int i = 1; i < vals.length; i++) {
        String pair[] = vals[i].split(":");
        assertTrue(result.containsColumn(familyNameBytes, Bytes.toBytes(pair[0])));
        assertEquals(pair[1], Bytes.toString(result.getValue(familyNameBytes, Bytes.toBytes(pair[0]))));
        assertEquals(1l, result.getColumn(familyNameBytes, Bytes.toBytes(pair[0])).get(0).getTimestamp());
      }
      index++;
    }
    assertEquals(data.length, index);
  }

  @Test
  public void directModeAbortTest() throws Exception {
    String testName = "directModeAbortTest";
    Path methodTestDir = new Path(getTestDir(), testName);
    String databaseName = testName;
    String dbDir = new Path(methodTestDir, "DB_" + testName).toString();
    String tableName = newTableName(testName);
    String familyName = "my_family";
    byte[] familyNameBytes = Bytes.toBytes(familyName);
    //Table name as specified by hbase.table.name property
    String hbaseTableName = tableName;

    // include hbase config in conf file
    Configuration conf = new Configuration(allConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));

    String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '" + dbDir
      + "'";
    String tableQuery = "CREATE TABLE " + databaseName + "." + tableName +
      "(key int, english string, spanish string) STORED BY " +
      "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'" +
      "TBLPROPERTIES (" +
      "'hbase.columns.mapping'=':key," + familyName + ":english," + familyName +
      ":spanish','hbase.table.name'='" + hbaseTableName + "')";

    assertEquals(0, hcatDriver.run(dbquery).getResponseCode());
    assertEquals(0, hcatDriver.run(tableQuery).getResponseCode());

    String data[] = {"1,english:ONE,spanish:UNO",
      "2,english:TWO,spanish:DOS",
      "3,english:THREE,spanish:TRES"};

    Path inputPath = new Path(methodTestDir, "mr_input");
    getFileSystem().mkdirs(inputPath);
    // create multiple files so we can test with multiple mappers
    for (int i = 0; i < data.length; i++) {
      FSDataOutputStream os = getFileSystem().create(
        new Path(inputPath, "inputFile" + i + ".txt"));
      os.write(Bytes.toBytes(data[i] + "\n"));
      os.close();
    }

    Path workingDir = new Path(methodTestDir, "mr_abort");
    OutputJobInfo outputJobInfo = OutputJobInfo.create(databaseName,
      tableName, null);
    Job job = configureJob(testName, conf, workingDir, MapWriteAbortTransaction.class,
      outputJobInfo, inputPath);
    assertFalse(job.waitForCompletion(true));

    // verify that revision manager has it as aborted transaction
    RevisionManager rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(conf);
    try {
      TableSnapshot snapshot = rm.createSnapshot(hbaseTableName);
      for (String family : snapshot.getColumnFamilies()) {
        assertEquals(1, snapshot.getRevision(family));
        List<FamilyRevision> abortedWriteTransactions = rm.getAbortedWriteTransactions(
          hbaseTableName, family);
        assertEquals(1, abortedWriteTransactions.size());
        assertEquals(1, abortedWriteTransactions.get(0).getRevision());
      }
    } finally {
      rm.close();
    }

    // verify that hbase has the records of the successful maps.
    HTable table = new HTable(conf, hbaseTableName);
    Scan scan = new Scan();
    scan.addFamily(familyNameBytes);
    ResultScanner scanner = table.getScanner(scan);
    int count = 0;
    for (Result result : scanner) {
      String key = Bytes.toString(result.getRow());
      assertNotSame(MapWriteAbortTransaction.failedKey, key);
      int index = Integer.parseInt(key) - 1;
      String vals[] = data[index].toString().split(",");
      for (int i = 1; i < vals.length; i++) {
        String pair[] = vals[i].split(":");
        assertTrue(result.containsColumn(familyNameBytes, Bytes.toBytes(pair[0])));
        assertEquals(pair[1],
          Bytes.toString(result.getValue(familyNameBytes, Bytes.toBytes(pair[0]))));
        assertEquals(1l, result.getColumn(familyNameBytes, Bytes.toBytes(pair[0])).get(0)
          .getTimestamp());
      }
      count++;
    }
    assertEquals(data.length - 1, count);

    // verify that the inputformat returns empty results.
    Path outputDir = new Path(getTestDir(),
      "mapred/testHBaseTableIgnoreAbortedTransactions");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    job = new Job(conf, "hbase-aborted-transaction");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadAbortedTransaction.class);
    job.setInputFormatClass(HCatInputFormat.class);
    HCatInputFormat.setInput(job, databaseName, tableName);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
  }

  private Job configureJob(String jobName, Configuration conf,
               Path workingDir, Class<? extends Mapper> mapperClass,
               OutputJobInfo outputJobInfo, Path inputPath) throws IOException {
    Job job = new Job(conf, jobName);
    job.setWorkingDirectory(workingDir);
    job.setJarByClass(this.getClass());
    job.setMapperClass(mapperClass);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, inputPath);
    job.setOutputFormatClass(HCatOutputFormat.class);
    HCatOutputFormat.setOutput(job, outputJobInfo);
    String txnString = job.getConfiguration().get(HBaseConstants.PROPERTY_WRITE_TXN_KEY);
    //Test passing in same OutputJobInfo multiple times and verify 1 transaction is created
    String jobString = job.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
    outputJobInfo = (OutputJobInfo) HCatUtil.deserialize(jobString);
    Job job2 = new Job(conf);
    HCatOutputFormat.setOutput(job2, outputJobInfo);
    assertEquals(txnString, job2.getConfiguration().get(HBaseConstants.PROPERTY_WRITE_TXN_KEY));
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(HCatRecord.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(HCatRecord.class);

    job.setNumReduceTasks(0);
    return job;
  }

  public static class MapHCatWrite extends Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      OutputJobInfo jobInfo = (OutputJobInfo) HCatUtil.deserialize(context.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
      HCatRecord record = new DefaultHCatRecord(3);
      HCatSchema schema = jobInfo.getOutputSchema();
      String vals[] = value.toString().split(",");
      record.setInteger("key", schema, Integer.parseInt(vals[0]));
      for (int i = 1; i < vals.length; i++) {
        String pair[] = vals[i].split(":");
        record.set(pair[0], schema, pair[1]);
      }
      context.write(null, record);
    }
  }

  public static class MapWrite implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, BytesWritable, Put> {

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<BytesWritable, Put> output, Reporter reporter)
      throws IOException {
      String vals[] = value.toString().split(",");
      Put put = new Put(Bytes.toBytes(vals[0]));
      for (int i = 1; i < vals.length; i++) {
        String pair[] = vals[i].split(":");
        put.add(Bytes.toBytes("my_family"),
          Bytes.toBytes(pair[0]),
          Bytes.toBytes(pair[1]));
      }
      output.collect(null, put);
    }
  }

  static class MapWriteAbortTransaction extends Mapper<LongWritable, Text, BytesWritable, HCatRecord> {
    public static String failedKey;
    private static int count = 0;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      OutputJobInfo jobInfo = (OutputJobInfo) HCatUtil.deserialize(context.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
      HCatRecord record = new DefaultHCatRecord(3);
      HCatSchema schema = jobInfo.getOutputSchema();
      String vals[] = value.toString().split(",");
      record.setInteger("key", schema, Integer.parseInt(vals[0]));
      synchronized (MapWriteAbortTransaction.class) {
        if (count == 2) {
          failedKey = vals[0];
          throw new IOException("Failing map to test abort");
        }
        for (int i = 1; i < vals.length; i++) {
          String pair[] = vals[i].split(":");
          record.set(pair[0], schema, pair[1]);
        }
        context.write(null, record);
        count++;
      }

    }

  }

  static class MapReadAbortedTransaction
    extends
    Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable<?>, Text> {

    @Override
    public void run(Context context) throws IOException,
      InterruptedException {
      setup(context);
      if (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(),
            context);
        }
        throw new IOException("There should have been no records");
      }
      cleanup(context);
    }

    @Override
    public void map(ImmutableBytesWritable key, HCatRecord value,
            Context context) throws IOException, InterruptedException {
      System.out.println("HCat record value" + value.toString());
    }
  }
}
