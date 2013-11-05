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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Shell;
import org.apache.hcatalog.NoExitSecurityManager;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.mapreduce.TestHCatPartitionPublish} instead
 */
public class TestHCatPartitionPublish {
  private static Configuration mrConf = null;
  private static FileSystem fs = null;
  private static MiniMRCluster mrCluster = null;
  private static boolean isServerRunning = false;
  private static int msPort;
  private static HiveConf hcatConf;
  private static HiveMetaStoreClient msc;
  private static SecurityManager securityManager;
  private static Configuration conf = new Configuration(true);

  @BeforeClass
  public static void setup() throws Exception {
    conf.set("yarn.scheduler.capacity.root.queues", "default");
    conf.set("yarn.scheduler.capacity.root.default.capacity", "100");

    fs = FileSystem.get(conf);
    System.setProperty("hadoop.log.dir", new File(fs.getWorkingDirectory()
        .toString(), "/logs").getAbsolutePath());
    // LocalJobRunner does not work with mapreduce OutputCommitter. So need
    // to use MiniMRCluster. MAPREDUCE-2350
    mrCluster = new MiniMRCluster(1, fs.getUri().toString(), 1, null, null,
        new JobConf(conf));
    mrConf = mrCluster.createJobConf();

    if (isServerRunning) {
      return;
    }

    msPort = MetaStoreUtils.findFreePort();

    MetaStoreUtils.startMetaStore(msPort, ShimLoader
        .getHadoopThriftAuthBridge());
    isServerRunning = true;
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());

    hcatConf = new HiveConf(TestHCatPartitionPublish.class);
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 120);
    hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
        "false");
    msc = new HiveMetaStoreClient(hcatConf, null);
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    System.setSecurityManager(securityManager);
    isServerRunning = false;
  }

  @Test
  public void testPartitionPublish() throws Exception {
    String dbName = "default";
    String tableName = "testHCatPartitionedTable";
    createTable(null, tableName);

    Map<String, String> partitionMap = new HashMap<String, String>();
    partitionMap.put("part1", "p1value1");
    partitionMap.put("part0", "p0value1");

    ArrayList<HCatFieldSchema> hcatTableColumns = new ArrayList<HCatFieldSchema>();
    for (FieldSchema fs : getTableColumns()) {
      hcatTableColumns.add(HCatSchemaUtils.getHCatFieldSchema(fs));
    }

    runMRCreateFail(dbName, tableName, partitionMap, hcatTableColumns);
    List<String> ptns = msc.listPartitionNames(dbName, tableName,
        (short) 10);
    Assert.assertEquals(0, ptns.size());
    Table table = msc.getTable(dbName, tableName);
    Assert.assertTrue(table != null);
    // In Windows, we cannot remove the output directory when job fail. See
    // FileOutputCommitterContainer.abortJob
    if (!Shell.WINDOWS) {
      Path path = new Path(table.getSd().getLocation()
          + "/part1=p1value1/part0=p0value1");
      Assert.assertFalse(path.getFileSystem(conf).exists(path));
    }
  }

  void runMRCreateFail(
    String dbName, String tableName, Map<String, String> partitionValues,
    List<HCatFieldSchema> columns) throws Exception {

    Job job = new Job(mrConf, "hcat mapreduce write fail test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(TestHCatPartitionPublish.MapFail.class);

    // input/output settings
    job.setInputFormatClass(TextInputFormat.class);

    Path path = new Path(fs.getWorkingDirectory(),
        "mapred/testHCatMapReduceInput");
    // The write count does not matter, as the map will fail in its first
    // call.
    createInputFile(path, 5);

    TextInputFormat.setInputPaths(job, path);
    job.setOutputFormatClass(HCatOutputFormat.class);
    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName,
        partitionValues);
    HCatOutputFormat.setOutput(job, outputJobInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(DefaultHCatRecord.class);

    job.setNumReduceTasks(0);

    HCatOutputFormat.setSchema(job, new HCatSchema(columns));

    boolean success = job.waitForCompletion(true);
    Assert.assertTrue(success == false);
  }

  private void createInputFile(Path path, int rowCount) throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    FSDataOutputStream os = fs.create(path);
    for (int i = 0; i < rowCount; i++) {
      os.writeChars(i + "\n");
    }
    os.close();
  }

  public static class MapFail extends
      Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      {
        throw new IOException("Exception to mimic job failure.");
      }
    }
  }

  private void createTable(String dbName, String tableName) throws Exception {
    String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME
        : dbName;
    try {
      msc.dropTable(databaseName, tableName);
    } catch (Exception e) {
    } // can fail with NoSuchObjectException

    Table tbl = new Table();
    tbl.setDbName(databaseName);
    tbl.setTableName(tableName);
    tbl.setTableType("MANAGED_TABLE");
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(getTableColumns());
    tbl.setPartitionKeys(getPartitionKeys());
    tbl.setSd(sd);
    sd.setBucketCols(new ArrayList<String>(2));
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(ColumnarSerDe.class.getName());
    sd.setInputFormat(RCFileInputFormat.class.getName());
    sd.setOutputFormat(RCFileOutputFormat.class.getName());

    Map<String, String> tableParams = new HashMap<String, String>();
    tbl.setParameters(tableParams);

    msc.createTable(tbl);
  }

  protected List<FieldSchema> getPartitionKeys() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    // Defining partition names in unsorted order
    fields.add(new FieldSchema("PaRT1", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("part0", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

  protected List<FieldSchema> getTableColumns() {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("c1", serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c2", serdeConstants.STRING_TYPE_NAME, ""));
    return fields;
  }

}
