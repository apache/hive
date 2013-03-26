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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

/**
 * Test for HCatOutputFormat. Writes a partition using HCatOutputFormat and reads
 * it back using HCatInputFormat, checks the column values and counts.
 */
public abstract class HCatMapReduceTest extends TestCase {

  protected String dbName = "default";
  protected String tableName = "testHCatMapReduceTable";

  protected String inputFormat = RCFileInputFormat.class.getName();
  protected String outputFormat = RCFileOutputFormat.class.getName();
  protected String serdeClass = ColumnarSerDe.class.getName();

  private static List<HCatRecord> writeRecords = new ArrayList<HCatRecord>();
  private static List<HCatRecord> readRecords = new ArrayList<HCatRecord>();

  protected abstract void initialize() throws Exception;

  protected abstract List<FieldSchema> getPartitionKeys();

  protected abstract List<FieldSchema> getTableColumns();

  private HiveMetaStoreClient client;
  protected HiveConf hiveConf;

  private FileSystem fs;
  private String thriftUri = null;

  protected Driver driver;

  @Override
  protected void setUp() throws Exception {
    hiveConf = new HiveConf(this.getClass());

    //The default org.apache.hadoop.hive.ql.hooks.PreExecutePrinter hook
    //is present only in the ql/test directory
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));

    thriftUri = System.getenv("HCAT_METASTORE_URI");

    if( thriftUri != null ) {
      System.out.println("Using URI " + thriftUri);

      hiveConf.set("hive.metastore.local", "false");
      hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUri);
    }

    fs = new LocalFileSystem();
    fs.initialize(fs.getWorkingDirectory().toUri(), new Configuration());

    initialize();

    client = new HiveMetaStoreClient(hiveConf, null);
    initTable();
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

      client.dropTable(databaseName, tableName);
    } catch(Exception e) {
      e.printStackTrace();
      throw e;
    }

    client.close();
  }



  private void initTable() throws Exception {

    String databaseName = (dbName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : dbName;

    try {
      client.dropTable(databaseName, tableName);
    } catch(Exception e) {
    } //can fail with NoSuchObjectException


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
    sd.getSerdeInfo().getParameters().put(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(serdeClass);
    sd.setInputFormat(inputFormat);
    sd.setOutputFormat(outputFormat);

    Map<String, String> tableParams = new HashMap<String, String>();
    tbl.setParameters(tableParams);

    client.createTable(tbl);
  }

  //Create test input file with specified number of rows
  private void createInputFile(Path path, int rowCount) throws IOException {

    if( fs.exists(path) ) {
      fs.delete(path, true);
    }

    FSDataOutputStream os = fs.create(path);

    for(int i = 0;i < rowCount;i++) {
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

        }catch(Exception e) {

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
        } catch(Exception e) {
          e.printStackTrace(); //print since otherwise exception is lost
          throw new IOException(e);
        }
      }
    }
  }

  void runMRCreate(Map<String, String> partitionValues,
        List<HCatFieldSchema> partitionColumns, List<HCatRecord> records,
        int writeCount, boolean assertWrite) throws Exception {

    writeRecords = records;
    MapCreate.writeCount = 0;

    Configuration conf = new Configuration();
    Job job = new Job(conf, "hcat mapreduce write test");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatMapReduceTest.MapCreate.class);

    // input/output settings
    job.setInputFormatClass(TextInputFormat.class);

    Path path = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceInput");
    createInputFile(path, writeCount);

    TextInputFormat.setInputPaths(job, path);

    job.setOutputFormatClass(HCatOutputFormat.class);

    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName, partitionValues);
    HCatOutputFormat.setOutput(job, outputJobInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(DefaultHCatRecord.class);

    job.setNumReduceTasks(0);

    HCatOutputFormat.setSchema(job, new HCatSchema(partitionColumns));

    boolean success = job.waitForCompletion(true);
    if (success) {
      new FileOutputCommitterContainer(job,null).commitJob(job);
    } else {
      new FileOutputCommitterContainer(job,null).abortJob(job, JobStatus.State.FAILED);
    }
    if (assertWrite){
      // we assert only if we expected to assert with this call.
      Assert.assertEquals(writeCount, MapCreate.writeCount);
    }
  }

  List<HCatRecord> runMRRead(int readCount) throws Exception {
    return runMRRead(readCount, null);
  }

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

    InputJobInfo inputJobInfo = InputJobInfo.create(dbName,tableName,filter);
    HCatInputFormat.setInput(job, inputJobInfo);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    Path path = new Path(fs.getWorkingDirectory(), "mapred/testHCatMapReduceOutput");
    if( fs.exists(path) ) {
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

    InputJobInfo inputJobInfo = InputJobInfo.create(dbName,tableName,null);
    HCatInputFormat.setInput(job, inputJobInfo);

    return HCatInputFormat.getTableSchema(job);
  }

}



