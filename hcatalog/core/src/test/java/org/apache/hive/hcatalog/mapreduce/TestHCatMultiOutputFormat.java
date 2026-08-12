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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.mapreduce.MultiOutputFormat.JobConfigurer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHCatMultiOutputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TestHCatMultiOutputFormat.class);

  private static final String DATABASE = "default";
  private static final String[] tableNames = {"test1", "test2", "test3"};
  private static final String[] tablePerms = {"755", "750", "700"};
  private static Path warehousedir = null;
  private static HashMap<String, HCatSchema> schemaMap = new HashMap<String, HCatSchema>();
  private static HiveMetaStoreClient hmsc;
  private static MiniMRCluster mrCluster;
  private static Configuration mrConf;
  private static HiveConf hiveConf;
  private static File workDir;

  static {
    schemaMap.put(tableNames[0], new HCatSchema(ColumnHolder.hCattest1Cols));
    schemaMap.put(tableNames[1], new HCatSchema(ColumnHolder.hCattest2Cols));
    schemaMap.put(tableNames[2], new HCatSchema(ColumnHolder.hCattest3Cols));
  }

  /**
   * Private class which holds all the data for the test cases
   */
  private static class ColumnHolder {

    private static ArrayList<HCatFieldSchema> hCattest1Cols = new ArrayList<HCatFieldSchema>();
    private static ArrayList<HCatFieldSchema> hCattest2Cols = new ArrayList<HCatFieldSchema>();
    private static ArrayList<HCatFieldSchema> hCattest3Cols = new ArrayList<HCatFieldSchema>();

    private static ArrayList<FieldSchema> partitionCols = new ArrayList<FieldSchema>();
    private static ArrayList<FieldSchema> test1Cols = new ArrayList<FieldSchema>();
    private static ArrayList<FieldSchema> test2Cols = new ArrayList<FieldSchema>();
    private static ArrayList<FieldSchema> test3Cols = new ArrayList<FieldSchema>();

    private static HashMap<String, List<FieldSchema>> colMapping = new HashMap<String, List<FieldSchema>>();

    static {
      try {
        FieldSchema keyCol = new FieldSchema("key", serdeConstants.STRING_TYPE_NAME, "");
        test1Cols.add(keyCol);
        test2Cols.add(keyCol);
        test3Cols.add(keyCol);
        hCattest1Cols.add(HCatSchemaUtils.getHCatFieldSchema(keyCol));
        hCattest2Cols.add(HCatSchemaUtils.getHCatFieldSchema(keyCol));
        hCattest3Cols.add(HCatSchemaUtils.getHCatFieldSchema(keyCol));
        FieldSchema valueCol = new FieldSchema("value", serdeConstants.STRING_TYPE_NAME, "");
        test1Cols.add(valueCol);
        test3Cols.add(valueCol);
        hCattest1Cols.add(HCatSchemaUtils.getHCatFieldSchema(valueCol));
        hCattest3Cols.add(HCatSchemaUtils.getHCatFieldSchema(valueCol));
        FieldSchema extraCol = new FieldSchema("extra", serdeConstants.STRING_TYPE_NAME, "");
        test3Cols.add(extraCol);
        hCattest3Cols.add(HCatSchemaUtils.getHCatFieldSchema(extraCol));
        colMapping.put("test1", test1Cols);
        colMapping.put("test2", test2Cols);
        colMapping.put("test3", test3Cols);
      } catch (HCatException e) {
        LOG.error("Error in setting up schema fields for the table", e);
        throw new RuntimeException(e);
      }
    }

    static {
      partitionCols.add(new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
      partitionCols.add(new FieldSchema("cluster", serdeConstants.STRING_TYPE_NAME, ""));
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    System.clearProperty("mapred.job.tracker");
    String testDir = System.getProperty("test.tmp.dir", "./");
    testDir = testDir + "/test_multitable_" + Math.abs(new Random().nextLong()) + "/";
    workDir = new File(new File(testDir).getCanonicalPath());
    FileUtil.fullyDelete(workDir);
    workDir.mkdirs();

    warehousedir = new Path(System.getProperty("test.warehouse.dir"));

    HiveConf metastoreConf = new HiveConf();
    metastoreConf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE, warehousedir.toString());

    // Run hive metastore server
    MetaStoreTestUtils.startMetaStoreWithRetry(metastoreConf);
    // Read the warehouse dir, which can be changed so multiple MetaStore tests could be run on
    // the same server
    warehousedir = new Path(MetastoreConf.getVar(metastoreConf, MetastoreConf.ConfVars.WAREHOUSE));
    // LocalJobRunner does not work with mapreduce OutputCommitter. So need
    // to use MiniMRCluster. MAPREDUCE-2350
    Configuration conf = new Configuration(true);
    conf.set("yarn.scheduler.capacity.root.queues", "default");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "100");

    FileSystem fs = FileSystem.get(conf);
    System.setProperty("hadoop.log.dir", new File(workDir, "/logs").getAbsolutePath());
    mrCluster = new MiniMRCluster(1, fs.getUri().toString(), 1, null, null,
      new JobConf(conf));
    mrConf = mrCluster.createJobConf();

    initializeSetup(metastoreConf);

    warehousedir.getFileSystem(conf).mkdirs(warehousedir);
  }

  private static void initializeSetup(HiveConf metastoreConf) throws Exception {

    hiveConf = new HiveConf(metastoreConf, TestHCatMultiOutputFormat.class);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_FAILURE_RETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
      HCatSemanticAnalyzer.class.getName());
    hiveConf.set(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    System.setProperty(HiveConf.ConfVars.PRE_EXEC_HOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POST_EXEC_HOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE));
    System.setProperty(HiveConf.ConfVars.METASTORE_CONNECT_URL_KEY.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.CONNECT_URL_KEY));
    System.setProperty(HiveConf.ConfVars.METASTORE_URIS.varname,
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.THRIFT_URIS));

    hiveConf.set(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, warehousedir.toString());
    try {
      hmsc = new HiveMetaStoreClient(hiveConf);
      initalizeTables();
    } catch (Throwable e) {
      LOG.error("Exception encountered while setting up testcase", e);
      throw new Exception(e);
    } finally {
      hmsc.close();
    }
  }

  private static void initalizeTables() throws Exception {
    for (String table : tableNames) {
      try {
        if (hmsc.getTable(DATABASE, table) != null) {
          hmsc.dropTable(DATABASE, table);
        }
      } catch (NoSuchObjectException ignored) {
      }
    }
    for (int i = 0; i < tableNames.length; i++) {
      createTable(tableNames[i], tablePerms[i]);
    }
  }

  private static void createTable(String tableName, String tablePerm) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(DATABASE);
    tbl.setTableName(tableName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(ColumnHolder.colMapping.get(tableName));
    tbl.setSd(sd);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
    sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(
      org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
    tbl.setPartitionKeys(ColumnHolder.partitionCols);

    hmsc.createTable(tbl);
    Path path = new Path(warehousedir, tableName);
    FileSystem fs = path.getFileSystem(hiveConf);
    fs.setPermission(path, new FsPermission(tablePerm));
  }

  @AfterClass
  public static void tearDown() throws IOException {
    FileUtil.fullyDelete(workDir);
    FileSystem fs = warehousedir.getFileSystem(hiveConf);
    if (fs.exists(warehousedir)) {
      fs.delete(warehousedir, true);
    }
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
  }

  /**
   * Simple test case.
   * <ol>
   * <li>Submits a mapred job which writes out one fixed line to each of the tables</li>
   * <li>uses hive fetch task to read the data and see if it matches what was written</li>
   * </ol>
   *
   * @throws Exception if any error occurs
   */
  @Test
  public void testOutputFormat() throws Throwable {
    HashMap<String, String> partitionValues = new HashMap<String, String>();
    partitionValues.put("ds", "1");
    partitionValues.put("cluster", "ag");
    ArrayList<OutputJobInfo> infoList = new ArrayList<OutputJobInfo>();
    infoList.add(OutputJobInfo.create("default", tableNames[0], partitionValues));
    infoList.add(OutputJobInfo.create("default", tableNames[1], partitionValues));
    infoList.add(OutputJobInfo.create("default", tableNames[2], partitionValues));

    // There are tests that check file permissions (which are manually set)
    // Disable NN ACLS so that the manual permissions are observed
    hiveConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, false);

    Job job = new Job(hiveConf, "SampleJob");

    job.setMapperClass(MyMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(MultiOutputFormat.class);
    job.setNumReduceTasks(0);

    JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);

    for (int i = 0; i < tableNames.length; i++) {
      configurer.addOutputFormat(tableNames[i], HCatOutputFormat.class, BytesWritable.class,
        HCatRecord.class);
      HCatOutputFormat.setOutput(configurer.getJob(tableNames[i]), infoList.get(i));
      HCatOutputFormat.setSchema(configurer.getJob(tableNames[i]),
        schemaMap.get(tableNames[i]));
    }
    configurer.configure();

    Path filePath = createInputFile();
    FileInputFormat.addInputPath(job, filePath);
    Assert.assertTrue(job.waitForCompletion(true));

    ArrayList<String> outputs = new ArrayList<String>();
    for (String tbl : tableNames) {
      outputs.add(getTableData(tbl, "default").get(0));
    }
    Assert.assertEquals("Comparing output of table " +
      tableNames[0] + " is not correct", outputs.get(0), "a,a,1,ag");
    Assert.assertEquals("Comparing output of table " +
      tableNames[1] + " is not correct", outputs.get(1),
      "a,1,ag");
    Assert.assertEquals("Comparing output of table " +
      tableNames[2] + " is not correct", outputs.get(2), "a,a,extra,1,ag");

    // Check permisssion on partition dirs and files created
    for (int i = 0; i < tableNames.length; i++) {
      final Path partitionFile = new Path(warehousedir + "/" + tableNames[i] + "/ds=1/cluster=ag/part-m-00000");

      final FileSystem fs = partitionFile.getFileSystem(mrConf);

      Assert.assertEquals("File permissions of table " + tableNames[i] + " is not correct [" + partitionFile + "]",
          new FsPermission(tablePerms[i]), fs.getFileStatus(partitionFile).getPermission());
      Assert.assertEquals(
          "File permissions of table " + tableNames[i] + " is not correct [" + partitionFile + "]",
          new FsPermission(tablePerms[i]), fs.getFileStatus(partitionFile).getPermission());
      Assert.assertEquals(
          "File permissions of table " + tableNames[i] + " is not correct [" +  partitionFile.getParent() + "]",
          new FsPermission(tablePerms[i]), fs.getFileStatus(partitionFile.getParent()).getPermission());

    }
    LOG.info("File permissions verified");
  }

  /**
   * Create a input file for map
   *
   * @return absolute path of the file.
   * @throws IOException if any error encountered
   */
  private Path createInputFile() throws IOException {
    Path f = new Path(workDir + "/MultiTableInput.txt");
    FileSystem fs = FileSystem.get(mrConf);
    if (fs.exists(f)) {
      fs.delete(f, true);
    }
    OutputStream out = fs.create(f);
    for (int i = 0; i < 3; i++) {
      out.write("a,a\n".getBytes());
    }
    out.close();
    return f;
  }

  /**
   * Method to fetch table data
   *
   * @param table table name
   * @param database database
   * @return list of columns in comma seperated way
   * @throws Exception if any error occurs
   */
  private List<String> getTableData(String table, String database) throws Exception {
    QueryState queryState = new QueryState.Builder().build();
    HiveConf conf = queryState.getConf();
    conf.addResource("hive-site.xml");
    ArrayList<String> results = new ArrayList<String>();
    ArrayList<String> temp = new ArrayList<String>();
    Hive hive = Hive.get(conf);
    org.apache.hadoop.hive.ql.metadata.Table tbl = hive.getTable(database, table);
    FetchWork work;
    if (!tbl.getPartCols().isEmpty()) {
      List<Partition> partitions = hive.getPartitions(tbl);
      List<PartitionDesc> partDesc = new ArrayList<PartitionDesc>();
      List<Path> partLocs = new ArrayList<Path>();
      TableDesc tableDesc = Utilities.getTableDesc(tbl);
      for (Partition part : partitions) {
        partLocs.add(part.getDataLocation());
        partDesc.add(Utilities.getPartitionDescFromTableDesc(tableDesc, part, true));
      }
      work = new FetchWork(partLocs, partDesc, tableDesc);
      work.setLimit(100);
    } else {
      work = new FetchWork(tbl.getDataLocation(), Utilities.getTableDesc(tbl));
    }
    FetchTask task = new FetchTask();
    task.setWork(work);
    conf.set("_hive.hdfs.session.path", "path");
    conf.set("_hive.local.session.path", "path");
    task.initialize(queryState, null, null, new org.apache.hadoop.hive.ql.Context(conf));
    task.execute();
    task.fetch(temp);
    for (String str : temp) {
      results.add(str.replace("\t", ","));
    }
    return results;
  }

  private static class MyMapper extends
    Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

    private int i = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      HCatRecord record = null;
      String[] splits = value.toString().split(",");
      switch (i) {
      case 0:
        record = new DefaultHCatRecord(2);
        record.set(0, splits[0]);
        record.set(1, splits[1]);
        break;
      case 1:
        record = new DefaultHCatRecord(1);
        record.set(0, splits[0]);
        break;
      case 2:
        record = new DefaultHCatRecord(3);
        record.set(0, splits[0]);
        record.set(1, splits[1]);
        record.set(2, "extra");
        break;
      default:
        Assert.fail("This should not happen!!!!!");
      }
      MultiOutputFormat.write(tableNames[i], null, record, context);
      i++;
    }
  }
}
