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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.hbase.ResultWritable;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.RevisionManagerConfiguration;
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.PartInfo;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHCatHBaseInputFormat extends SkeletonHBaseTest {

  private static HiveConf hcatConf;
  private static HCatDriver hcatDriver;
  private final byte[] FAMILY = Bytes.toBytes("testFamily");
  private final byte[] QUALIFIER1 = Bytes.toBytes("testQualifier1");
  private final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  public TestHCatHBaseInputFormat() throws Exception {
    hcatConf = getHiveConf();
    hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    URI fsuri = getFileSystem().getUri();
    Path whPath = new Path(fsuri.getScheme(), fsuri.getAuthority(),
        getTestDir());
    hcatConf.set(HiveConf.ConfVars.HADOOPFS.varname, fsuri.toString());
    hcatConf.set(ConfVars.METASTOREWAREHOUSE.varname, whPath.toString());

    //Add hbase properties

    for (Map.Entry<String, String> el : getHbaseConf()) {
      if (el.getKey().startsWith("hbase.")) {
        hcatConf.set(el.getKey(), el.getValue());
      }
    }
    HBaseConfiguration.merge(hcatConf,
        RevisionManagerConfiguration.create());


    SessionState.start(new CliSessionState(hcatConf));
    hcatDriver = new HCatDriver();

  }

  private List<Put> generatePuts(int num, String tableName) throws IOException {

    List<String> columnFamilies = Arrays.asList("testFamily");
    RevisionManager rm = null;
    List<Put> myPuts;
    try {
      rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(hcatConf);
      rm.open();
      myPuts = new ArrayList<Put>();
      for (int i = 1; i <= num; i++) {
        Put put = new Put(Bytes.toBytes("testRow"));
        put.add(FAMILY, QUALIFIER1, i, Bytes.toBytes("textValue-" + i));
        put.add(FAMILY, QUALIFIER2, i, Bytes.toBytes("textValue-" + i));
        myPuts.add(put);
        Transaction tsx = rm.beginWriteTransaction(tableName,
            columnFamilies);
        rm.commitWriteTransaction(tsx);
      }
    } finally {
      if (rm != null)
        rm.close();
    }

    return myPuts;
  }

  private void populateHBaseTable(String tName, int revisions) throws IOException {
    List<Put> myPuts = generatePuts(revisions, tName);
    HTable table = new HTable(getHbaseConf(), Bytes.toBytes(tName));
    table.put(myPuts);
  }

  private long populateHBaseTableQualifier1(String tName, int value, Boolean commit)
    throws IOException {
    List<String> columnFamilies = Arrays.asList("testFamily");
    RevisionManager rm = null;
    List<Put> myPuts = new ArrayList<Put>();
    long revision;
    try {
      rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(hcatConf);
      rm.open();
      Transaction tsx = rm.beginWriteTransaction(tName, columnFamilies);

      Put put = new Put(Bytes.toBytes("testRow"));
      revision = tsx.getRevisionNumber();
      put.add(FAMILY, QUALIFIER1, revision,
          Bytes.toBytes("textValue-" + value));
      myPuts.add(put);

      // If commit is null it is left as a running transaction
      if (commit != null) {
        if (commit) {
          rm.commitWriteTransaction(tsx);
        } else {
          rm.abortWriteTransaction(tsx);
        }
      }
    } finally {
      if (rm != null)
        rm.close();
    }
    HTable table = new HTable(getHbaseConf(), Bytes.toBytes(tName));
    table.put(myPuts);
    return revision;
  }

  @Test
  public void TestHBaseTableReadMR() throws Exception {
    String tableName = newTableName("MyTable");
    String databaseName = newTableName("MyDatabase");
    //Table name will be lower case unless specified by hbase.table.name property
    String hbaseTableName = (databaseName + "." + tableName).toLowerCase();
    String db_dir = getTestDir() + "/hbasedb";

    String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
        + db_dir + "'";
    String tableQuery = "CREATE TABLE " + databaseName + "." + tableName
        + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
        "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'=':key,testFamily:testQualifier1,testFamily:testQualifier2')";

    CommandProcessorResponse responseOne = hcatDriver.run(dbquery);
    assertEquals(0, responseOne.getResponseCode());
    CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
    assertEquals(0, responseTwo.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists(hbaseTableName);
    assertTrue(doesTableExist);

    populateHBaseTable(hbaseTableName, 5);
    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(getHiveConf().getAllProperties()));

    // output settings
    Path outputDir = new Path(getTestDir(), "mapred/testHbaseTableMRRead");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    // create job
    Job job = new Job(conf, "hbase-mr-read-test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadHTable.class);
    MapReadHTable.resetCounters();

    job.setInputFormatClass(HCatInputFormat.class);
    HCatInputFormat.setInput(job.getConfiguration(), databaseName, tableName);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
    // Note: These asserts only works in case of LocalJobRunner as they run in same jvm.
    // If using MiniMRCluster, the tests will have to be modified.
    assertFalse(MapReadHTable.error);
    assertEquals(1, MapReadHTable.count);

    String dropTableQuery = "DROP TABLE " + hbaseTableName;
    CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
    assertEquals(0, responseThree.getResponseCode());

    boolean isHbaseTableThere = hAdmin.tableExists(hbaseTableName);
    assertFalse(isHbaseTableThere);

    String dropDB = "DROP DATABASE " + databaseName;
    CommandProcessorResponse responseFour = hcatDriver.run(dropDB);
    assertEquals(0, responseFour.getResponseCode());
  }

  @Test
  public void TestHBaseTableProjectionReadMR() throws Exception {

    String tableName = newTableName("MyTable");
    //Table name as specified by hbase.table.name property
    String hbaseTableName = "MyDB_" + tableName;
    String tableQuery = "CREATE TABLE " + tableName
        + "(key string, testqualifier1 string, testqualifier2 string) STORED BY "
        + "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'="
        + "':key,testFamily:testQualifier1,testFamily:testQualifier2',"
        + "'hbase.table.name'='" + hbaseTableName + "')";

    CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
    assertEquals(0, responseTwo.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists(hbaseTableName);
    assertTrue(doesTableExist);

    populateHBaseTable(hbaseTableName, 5);

    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(getHiveConf().getAllProperties()));

    // output settings
    Path outputDir = new Path(getTestDir(), "mapred/testHBaseTableProjectionReadMR");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    // create job
    Job job = new Job(conf, "hbase-column-projection");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadProjHTable.class);
    job.setInputFormatClass(HCatInputFormat.class);
    HCatInputFormat.setOutputSchema(job, getProjectionSchema());
    HCatInputFormat.setInput(job, MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
    assertFalse(MapReadProjHTable.error);
    assertEquals(1, MapReadProjHTable.count);

    String dropTableQuery = "DROP TABLE " + tableName;
    CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
    assertEquals(0, responseThree.getResponseCode());

    boolean isHbaseTableThere = hAdmin.tableExists(hbaseTableName);
    assertFalse(isHbaseTableThere);
  }

  @Test
  public void TestHBaseInputFormatProjectionReadMR() throws Exception {

    String tableName = newTableName("mytable");
    String tableQuery = "CREATE TABLE " + tableName
        + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
        "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'=':key," +
        "testFamily:testQualifier1,testFamily:testQualifier2')";

    CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
    assertEquals(0, responseTwo.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists(tableName);
    assertTrue(doesTableExist);

    populateHBaseTable(tableName, 5);

    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(getHiveConf().getAllProperties()));

    // output settings
    Path outputDir = new Path(getTestDir(), "mapred/testHBaseInputFormatProjectionReadMR");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    // create job
    JobConf job = new JobConf(conf);
    job.setJobName("hbase-scan-column");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadProjectionHTable.class);
    job.setInputFormat(HBaseInputFormat.class);

    //Configure projection schema
    job.set(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA, HCatUtil.serialize(getProjectionSchema()));
    Job newJob = new Job(job);
    HCatInputFormat.setInput(newJob, MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    String inputJobString = newJob.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO);
    InputJobInfo info = (InputJobInfo) HCatUtil.deserialize(inputJobString);
    job.set(HCatConstants.HCAT_KEY_JOB_INFO, inputJobString);
    for (PartInfo partinfo : info.getPartitions()) {
      for (Entry<String, String> entry : partinfo.getJobProperties().entrySet())
        job.set(entry.getKey(), entry.getValue());
    }
    assertEquals("testFamily:testQualifier1", job.get(TableInputFormat.SCAN_COLUMNS));

    job.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
    org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    RunningJob runJob = JobClient.runJob(job);
    runJob.waitForCompletion();
    assertTrue(runJob.isSuccessful());
    assertFalse(MapReadProjectionHTable.error);
    assertEquals(1, MapReadProjectionHTable.count);

    String dropTableQuery = "DROP TABLE " + tableName;
    CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
    assertEquals(0, responseThree.getResponseCode());

    boolean isHbaseTableThere = hAdmin.tableExists(tableName);
    assertFalse(isHbaseTableThere);
  }

  @Test
  public void TestHBaseTableIgnoreAbortedTransactions() throws Exception {
    String tableName = newTableName("mytable");
    String tableQuery = "CREATE TABLE " + tableName
        + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
        "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'=':key," +
        "testFamily:testQualifier1,testFamily:testQualifier2')";

    CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
    assertEquals(0, responseTwo.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists(tableName);
    assertTrue(doesTableExist);

    populateHBaseTable(tableName, 5);
    populateHBaseTableQualifier1(tableName, 6, false);
    populateHBaseTableQualifier1(tableName, 7, false);

    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(getHiveConf().getAllProperties()));

    Path outputDir = new Path(getTestDir(), "mapred/testHBaseTableIgnoreAbortedTransactions");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    Job job = new Job(conf, "hbase-aborted-transaction");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadHTable.class);
    MapReadHTable.resetCounters();
    job.setInputFormatClass(HCatInputFormat.class);
    HCatInputFormat.setInput(job, MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
    // Verify that the records do not contain aborted transaction
    // revisions 6 and 7 for testFamily:testQualifier1 and
    // fetches revision 5 for both testQualifier1 and testQualifier2
    assertFalse(MapReadHTable.error);
    assertEquals(1, MapReadHTable.count);

    String dropTableQuery = "DROP TABLE " + tableName;
    CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
    assertEquals(0, responseThree.getResponseCode());

    boolean isHbaseTableThere = hAdmin.tableExists(tableName);
    assertFalse(isHbaseTableThere);
  }

  @Test
  public void TestHBaseTableIgnoreAbortedAndRunningTransactions() throws Exception {
    String tableName = newTableName("mytable");
    String tableQuery = "CREATE TABLE " + tableName
        + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
        "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
        + "TBLPROPERTIES ('hbase.columns.mapping'=':key," +
        "testFamily:testQualifier1,testFamily:testQualifier2')";

    CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
    assertEquals(0, responseTwo.getResponseCode());

    HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
    boolean doesTableExist = hAdmin.tableExists(tableName);
    assertTrue(doesTableExist);

    populateHBaseTable(tableName, 2);
    populateHBaseTableQualifier1(tableName, 3, Boolean.TRUE); //Committed transaction
    populateHBaseTableQualifier1(tableName, 4, null); //Running transaction
    populateHBaseTableQualifier1(tableName, 5, Boolean.FALSE);  //Aborted transaction
    populateHBaseTableQualifier1(tableName, 6, Boolean.TRUE); //Committed transaction
    populateHBaseTableQualifier1(tableName, 7, null); //Running Transaction
    populateHBaseTableQualifier1(tableName, 8, Boolean.FALSE); //Aborted Transaction

    Configuration conf = new Configuration(hcatConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF,
        HCatUtil.serialize(getHiveConf().getAllProperties()));

    Path outputDir = new Path(getTestDir(), "mapred/testHBaseTableIgnoreAbortedTransactions");
    FileSystem fs = getFileSystem();
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true);
    }
    Job job = new Job(conf, "hbase-running-aborted-transaction");
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapReadHTableRunningAbort.class);
    job.setInputFormatClass(HCatInputFormat.class);
    HCatInputFormat.setInput(job, MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    assertTrue(job.waitForCompletion(true));
    // Verify that the records do not contain running and aborted transaction
    // and it fetches revision 2 for testQualifier1 and testQualifier2
    assertFalse(MapReadHTableRunningAbort.error);
    assertEquals(1, MapReadHTableRunningAbort.count);

    String dropTableQuery = "DROP TABLE " + tableName;
    CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
    assertEquals(0, responseThree.getResponseCode());

    boolean isHbaseTableThere = hAdmin.tableExists(tableName);
    assertFalse(isHbaseTableThere);
  }


  static class MapReadHTable
    extends
    Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable<?>, Text> {

    static boolean error = false;
    static int count = 0;

    @Override
    public void map(ImmutableBytesWritable key, HCatRecord value,
        Context context) throws IOException, InterruptedException {
      System.out.println("HCat record value" + value.toString());
      boolean correctValues = (value.size() == 3)
          && (value.get(0).toString()).equalsIgnoreCase("testRow")
          && (value.get(1).toString()).equalsIgnoreCase("textValue-5")
          && (value.get(2).toString()).equalsIgnoreCase("textValue-5");

      if (correctValues == false) {
        error = true;
      }
      count++;
    }

    public static void resetCounters() {
      error = false;
      count = 0;
    }
  }

  static class MapReadProjHTable
    extends
    Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable<?>, Text> {

    static boolean error = false;
    static int count = 0;

    @Override
    public void map(ImmutableBytesWritable key, HCatRecord value,
        Context context) throws IOException, InterruptedException {
      System.out.println("HCat record value" + value.toString());
      boolean correctValues = (value.size() == 2)
          && (value.get(0).toString()).equalsIgnoreCase("testRow")
          && (value.get(1).toString()).equalsIgnoreCase("textValue-5");

      if (correctValues == false) {
        error = true;
      }
      count++;
    }
  }

  static class MapReadProjectionHTable
    implements org.apache.hadoop.mapred.Mapper<ImmutableBytesWritable, Object, WritableComparable<?>, Text> {

    static boolean error = false;
    static int count = 0;

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void map(ImmutableBytesWritable key, Object resultObj,
        OutputCollector<WritableComparable<?>, Text> output, Reporter reporter)
      throws IOException {
      Result result;
      if (resultObj instanceof Result){
        result = (Result) resultObj;
      } else if (resultObj instanceof ResultWritable) {
        result = ((ResultWritable)resultObj).getResult();
      } else {
        throw new IllegalArgumentException("Illegal Argument " + (resultObj == null ? "null" : resultObj.getClass().getName()));
      }
      System.out.println("Result " + result.toString());
      List<KeyValue> list = result.list();
      boolean correctValues = (list.size() == 1)
          && (Bytes.toString(list.get(0).getRow())).equalsIgnoreCase("testRow")
          && (Bytes.toString(list.get(0).getValue())).equalsIgnoreCase("textValue-5")
          && (Bytes.toString(list.get(0).getFamily())).equalsIgnoreCase("testFamily")
          && (Bytes.toString(list.get(0).getQualifier())).equalsIgnoreCase("testQualifier1");

      if (correctValues == false) {
        error = true;
      }
      count++;
    }
  }

  static class MapReadHTableRunningAbort
    extends
    Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable<?>, Text> {

    static boolean error = false;
    static int count = 0;

    @Override
    public void map(ImmutableBytesWritable key, HCatRecord value,
        Context context) throws IOException, InterruptedException {
      System.out.println("HCat record value" + value.toString());
      boolean correctValues = (value.size() == 3)
          && (value.get(0).toString()).equalsIgnoreCase("testRow")
          && (value.get(1).toString()).equalsIgnoreCase("textValue-3")
          && (value.get(2).toString()).equalsIgnoreCase("textValue-2");

      if (correctValues == false) {
        error = true;
      }
      count++;
    }
  }

  private HCatSchema getProjectionSchema() throws HCatException {

    HCatSchema schema = new HCatSchema(new ArrayList<HCatFieldSchema>());
    schema.append(new HCatFieldSchema("key", HCatFieldSchema.Type.STRING,
        ""));
    schema.append(new HCatFieldSchema("testqualifier1",
        HCatFieldSchema.Type.STRING, ""));
    return schema;
  }


}
