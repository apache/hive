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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

/**
 * Test for HCatOutputFormat. Writes a partition using HCatOutputFormat and reads
 * it back using HCatInputFormat, checks the column values and counts.
 */
public abstract class HCatMapReduceTest extends HCatBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(HCatMapReduceTest.class);
  protected static String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
  protected static String tableName = "testHCatMapReduceTable";

  private static List<HCatRecord> writeRecords = new ArrayList<HCatRecord>();
  private static List<HCatRecord> readRecords = new ArrayList<HCatRecord>();

  protected abstract List<FieldSchema> getPartitionKeys();

  protected abstract List<FieldSchema> getTableColumns();

  private static FileSystem fs;
  private String externalTableLocation = null;

  protected Boolean isTableExternal() {
    return false;
  }

  protected boolean isTableImmutable() {
    return true;
  }

  protected String inputFormat() {
    return RCFileInputFormat.class.getName();
  }

  protected String outputFormat() { 
    return RCFileOutputFormat.class.getName(); 
  }

  protected String serdeClass() { 
    return ColumnarSerDe.class.getName(); 
  }

  @BeforeClass
  public static void setUpOneTime() throws Exception {
    fs = new LocalFileSystem();
    fs.initialize(fs.getWorkingDirectory().toUri(), new Configuration());

    HiveConf hiveConf = new HiveConf();
    hiveConf.setInt(HCatConstants.HCAT_HIVE_CLIENT_EXPIRY_TIME, 0);
    // Hack to initialize cache with 0 expiry time causing it to return a new hive client every time
    // Otherwise the cache doesn't play well with the second test method with the client gets closed() in the
    // tearDown() of the previous test
    HCatUtil.getHiveClient(hiveConf);

    MapCreate.writeCount = 0;
    MapRead.readCount = 0;
  }

  @After
  public void deleteTable() throws Exception {
    try {
      String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

      client.dropTable(databaseName, tableName);
      // in case of external table, drop the table contents as well
      if (isTableExternal() && (externalTableLocation != null)) {
        if (fs.exists(new Path(externalTableLocation))) {
          fs.delete(new Path(externalTableLocation), true);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @Before
  public void createTable() throws Exception {
    String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

    try {
      client.dropTable(databaseName, tableName);
    } catch (Exception e) {
    } //can fail with NoSuchObjectException


    Table tbl = new Table();
    tbl.setDbName(databaseName);
    tbl.setTableName(tableName);
    if (isTableExternal()){
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }
    StorageDescriptor sd = new StorageDescriptor();

    sd.setCols(getTableColumns());
    tbl.setPartitionKeys(getPartitionKeys());

    tbl.setSd(sd);

    sd.setBucketCols(new ArrayList<String>(2));
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    if (isTableExternal()){
      sd.getSerdeInfo().getParameters().put("EXTERNAL", "TRUE");
    }
    sd.getSerdeInfo().setSerializationLib(serdeClass());
    sd.setInputFormat(inputFormat());
    sd.setOutputFormat(outputFormat());

    Map<String, String> tableParams = new HashMap<String, String>();
    if (isTableExternal()) {
      tableParams.put("EXTERNAL", "TRUE");
    }
    if (isTableImmutable()){
      tableParams.put(hive_metastoreConstants.IS_IMMUTABLE,"true");
    }
    tbl.setParameters(tableParams);

    client.createTable(tbl);
  }

  //Create test input file with specified number of rows
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

  public static class MapCreate extends
      Mapper<LongWritable, Text, BytesWritable, HCatRecord> {

    static int writeCount = 0; //test will be in local mode

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      {
        try {
          HCatRecord rec = writeRecords.get(writeCount);
          context.write(null, rec);
          writeCount++;

        } catch (Exception e) {

          e.printStackTrace(System.err); //print since otherwise exception is lost
          throw new IOException(e);
        }
      }
    }
  }

  public static class MapRead extends
      Mapper<WritableComparable, HCatRecord, BytesWritable, Text> {

    static int readCount = 0; //test will be in local mode

    @Override
    public void map(WritableComparable key, HCatRecord value, Context context
    ) throws IOException, InterruptedException {
      {
        try {
          readRecords.add(value);
          readCount++;
        } catch (Exception e) {
          e.printStackTrace(); //print since otherwise exception is lost
          throw new IOException(e);
        }
      }
    }
  }

  Job runMRCreate(Map<String, String> partitionValues,
          List<HCatFieldSchema> partitionColumns, List<HCatRecord> records,
          int writeCount, boolean assertWrite) throws Exception {
    return runMRCreate(partitionValues, partitionColumns, records, writeCount, assertWrite,
        true, null);
  }

  /**
   * Run a local map reduce job to load data from in memory records to an HCatalog Table
   * @param partitionValues
   * @param partitionColumns
   * @param records data to be written to HCatalog table
   * @param writeCount
   * @param assertWrite
   * @param asSingleMapTask
   * @return
   * @throws Exception
   */
  Job runMRCreate(Map<String, String> partitionValues,
          List<HCatFieldSchema> partitionColumns, List<HCatRecord> records,
          int writeCount, boolean assertWrite, boolean asSingleMapTask,
          String customDynamicPathPattern) throws Exception {

    writeRecords = records;
    MapCreate.writeCount = 0;

    Configuration conf = new Configuration();
    Job job = new Job(conf, "hcat mapreduce write test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatMapReduceTest.MapCreate.class);

    // input/output settings
    job.setInputFormatClass(TextInputFormat.class);

    if (asSingleMapTask) {
      // One input path would mean only one map task
      Path path = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceInput");
      createInputFile(path, writeCount);
      TextInputFormat.setInputPaths(job, path);
    } else {
      // Create two input paths so that two map tasks get triggered. There could be other ways
      // to trigger two map tasks.
      Path path = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceInput");
      createInputFile(path, writeCount / 2);

      Path path2 = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceInput2");
      createInputFile(path2, (writeCount - writeCount / 2));

      TextInputFormat.setInputPaths(job, path, path2);
    }

    job.setOutputFormatClass(HCatOutputFormat.class);

    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName, partitionValues);
    if (customDynamicPathPattern != null) {
      job.getConfiguration().set(HCatConstants.HCAT_DYNAMIC_CUSTOM_PATTERN, customDynamicPathPattern);
    }
    HCatOutputFormat.setOutput(job, outputJobInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(DefaultHCatRecord.class);

    job.setNumReduceTasks(0);

    HCatOutputFormat.setSchema(job, new HCatSchema(partitionColumns));

    boolean success = job.waitForCompletion(true);

    // Ensure counters are set when data has actually been read.
    if (partitionValues != null) {
      assertTrue(job.getCounters().getGroup("FileSystemCounters")
          .findCounter("FILE_BYTES_READ").getValue() > 0);
    }

    if (!HCatUtil.isHadoop23()) {
      // Local mode outputcommitter hook is not invoked in Hadoop 1.x
      if (success) {
        new FileOutputCommitterContainer(job, null).commitJob(job);
      } else {
        new FileOutputCommitterContainer(job, null).abortJob(job, JobStatus.State.FAILED);
      }
    }
    if (assertWrite) {
      // we assert only if we expected to assert with this call.
      Assert.assertEquals(writeCount, MapCreate.writeCount);
    }

    if (isTableExternal()) {
      externalTableLocation = outputJobInfo.getTableInfo().getTableLocation();
    }

    return job;
  }

  List<HCatRecord> runMRRead(int readCount) throws Exception {
    return runMRRead(readCount, null);
  }

  /**
   * Run a local map reduce job to read records from HCatalog table and verify if the count is as expected
   * @param readCount
   * @param filter
   * @return
   * @throws Exception
   */
  List<HCatRecord> runMRRead(int readCount, String filter) throws Exception {

    MapRead.readCount = 0;
    readRecords.clear();

    Configuration conf = new Configuration();
    Job job = new Job(conf, "hcat mapreduce read test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatMapReduceTest.MapRead.class);

    // input/output settings
    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HCatInputFormat.setInput(job, dbName, tableName, filter);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    Path path = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceOutput");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    TextOutputFormat.setOutputPath(job, path);

    job.waitForCompletion(true);
    Assert.assertEquals(readCount, MapRead.readCount);

    return readRecords;
  }


  protected HCatSchema getTableSchema() throws Exception {

    Configuration conf = new Configuration();
    Job job = new Job(conf, "hcat mapreduce read schema test");
    job.setJarByClass(this.getClass());

    // input/output settings
    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HCatInputFormat.setInput(job, dbName, tableName);

    return HCatInputFormat.getTableSchema(job.getConfiguration());
  }

}



