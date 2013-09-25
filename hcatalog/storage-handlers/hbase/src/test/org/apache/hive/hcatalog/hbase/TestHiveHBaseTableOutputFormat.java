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

package org.apache.hive.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.hbase.HiveHBaseTableOutputFormat;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hive.hcatalog.cli.HCatDriver;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test HBaseDirectOUtputFormat and HBaseStorageHandler using a MiniCluster
 */
public class TestHiveHBaseTableOutputFormat extends SkeletonHBaseTest {

  private final HiveConf allConf;
  private final HCatDriver hcatDriver;

  public TestHiveHBaseTableOutputFormat() {
    allConf = getHiveConf();
    allConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    allConf.set(HiveConf.ConfVars.HADOOPFS.varname, getFileSystem().getUri().toString());
    allConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new Path(getTestDir(),"warehouse").toString());

    //Add hbase properties
    for (Map.Entry<String, String> el : getHbaseConf())
      if (el.getKey().startsWith("hbase.")) {
        allConf.set(el.getKey(), el.getValue());
      }
    SessionState.start(new CliSessionState(allConf));
    hcatDriver = new HCatDriver();
  }

  @Test
  public void directOutputFormatTest() throws IOException, ClassNotFoundException, InterruptedException {
    String testName = "directOutputFormatTest";
    Path methodTestDir = new Path(getTestDir(),testName);

    String tableName = newTableName(testName).toLowerCase();
    String familyName = "my_family";
    byte[] familyNameBytes = Bytes.toBytes(familyName);

    //include hbase config in conf file
    Configuration conf = new Configuration(allConf);
    conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));

    //create table
    createTable(tableName,new String[]{familyName});

    String data[] = {
      "1,english:ONE,spanish:UNO",
      "2,english:TWO,spanish:DOS",
      "3,english:THREE,spanish:TRES"};

    // input/output settings
    Path inputPath = new Path(methodTestDir,"mr_input");
    getFileSystem().mkdirs(inputPath);
    FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
    for(String line: data)
      os.write(Bytes.toBytes(line + "\n"));
    os.close();

    //create job
    JobConf job = new JobConf(conf);
    job.setJobName(testName);
    job.setWorkingDirectory(new Path(methodTestDir,"mr_work"));
    job.setJarByClass(this.getClass());
    job.setMapperClass(MapWrite.class);

    job.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
    org.apache.hadoop.mapred.TextInputFormat.setInputPaths(job, inputPath);
    // why we need to set all the 3 properties??
    job.setOutputFormat(HiveHBaseTableOutputFormat.class);
    job.set(HBaseSerDe.HBASE_TABLE_NAME,tableName);
    job.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    job.set(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX+".hbase.mapreduce.outputTableName", tableName);

    try {
      OutputJobInfo outputJobInfo = OutputJobInfo.create("default", tableName, null);
      job.set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
          HCatUtil.serialize(outputJobInfo));
    } catch (Exception ex) {
      throw new IOException("Serialization error " + ex.getMessage(), ex);
    }

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(HCatRecord.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(HCatRecord.class);
    job.setNumReduceTasks(0);
    System.getProperty("java.classpath");
    RunningJob runJob = JobClient.runJob(job);
    runJob.waitForCompletion();
    assertTrue(runJob.isSuccessful());

    //verify
    HTable table = new HTable(conf, tableName);
    Scan scan = new Scan();
    scan.addFamily(familyNameBytes);
    ResultScanner scanner = table.getScanner(scan);
    int index=0;
    for(Result result: scanner) {
      String vals[] = data[index].toString().split(",");
      for(int i=1;i<vals.length;i++) {
        String pair[] = vals[i].split(":");
        assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes(pair[0])));
        assertEquals(pair[1],Bytes.toString(result.getValue(familyNameBytes,Bytes.toBytes(pair[0]))));
      }
      index++;
    }
    assertEquals(data.length,index);
  }

  @Test
  public void directHCatOutputFormatTest() throws Exception {
    String testName = "TestHiveHBaseTableOutputFormat";
    Path methodTestDir = new Path(getTestDir(),testName);

    String databaseName = testName;
    String dbDir = new Path(methodTestDir,"DB_"+testName).toString();
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
        "'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" +
        " WITH  SERDEPROPERTIES (" +
        "'hbase.columns.mapping'=':key,"+familyName+":english,"+familyName+":spanish')" ;

    assertEquals(0, hcatDriver.run(dbquery).getResponseCode());
    assertEquals(0, hcatDriver.run(tableQuery).getResponseCode());

    String data[] = {
      "1,english:ONE,spanish:UNO",
      "2,english:TWO,spanish:DOS",
      "3,english:THREE,spanish:TRES"};

    // input/output settings
    Path inputPath = new Path(methodTestDir,"mr_input");
    getFileSystem().mkdirs(inputPath);
    //create multiple files so we can test with multiple mappers
    for(int i=0;i<data.length;i++) {
      FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile"+i+".txt"));
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


    //verify
    HTable table = new HTable(conf, hbaseTableName);
    Scan scan = new Scan();
    scan.addFamily(familyNameBytes);
    ResultScanner scanner = table.getScanner(scan);
    int index=0;
    for(Result result: scanner) {
      String vals[] = data[index].toString().split(",");
      for(int i=1;i<vals.length;i++) {
        String pair[] = vals[i].split(":");
        assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes(pair[0])));
        assertEquals(pair[1],Bytes.toString(result.getValue(familyNameBytes,Bytes.toBytes(pair[0]))));
        //assertEquals(1l,result.getColumn(familyNameBytes,Bytes.toBytes(pair[0])).get(0).getTimestamp());
      }
      index++;
    }
    assertEquals(data.length,index);
  }

  private Job configureJob(String jobName, Configuration conf,
      Path workingDir, Class<? extends Mapper> mapperClass,
      OutputJobInfo outputJobInfo, Path inputPath) throws IOException {

    try {
      //now setting the schema
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      HiveMetaStoreClient client = HCatUtil.getHiveClient(hiveConf);
      Table table = client.getTable(outputJobInfo.getDatabaseName(), outputJobInfo.getTableName());
      StorageDescriptor tblSD = table.getSd();
      if (tblSD == null) {
        throw new HCatException(
            "Cannot construct partition info from an empty storage descriptor.");
      }
      HCatSchema tableSchema = new HCatSchema(HCatUtil.getHCatFieldSchemaList(tblSD.getCols()));
      outputJobInfo.setOutputSchema(tableSchema);
    }
    catch(Exception e) {
      if( e instanceof HCatException ) {
        throw (HCatException) e;
      } else {
        throw new HCatException(ErrorType.ERROR_SET_OUTPUT, e);
      }
    }
    conf.set(HBaseSerDe.HBASE_TABLE_NAME,outputJobInfo.getDatabaseName()+ "." + outputJobInfo.getTableName());
    conf.set(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,outputJobInfo.getDatabaseName()+ "." + outputJobInfo.getTableName());
    conf.set(TableOutputFormat.OUTPUT_TABLE, outputJobInfo.getDatabaseName() + "."+ outputJobInfo.getTableName());
    conf.set(HCatConstants.HCAT_DEFAULT_TOPIC_PREFIX+".hbase.mapreduce.outputTableName", outputJobInfo.getDatabaseName() + "." + outputJobInfo.getTableName());
    conf.set(HCatConstants.HCAT_KEY_OUTPUT_INFO,HCatUtil.serialize(outputJobInfo));

    Job job = new Job(conf, jobName);
    job.setWorkingDirectory(workingDir);
    job.setJarByClass(this.getClass());
    job.setMapperClass(mapperClass);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, inputPath);
    //job.setOutputFormatClass(HiveHBaseTableOutputFormat.class);
    job.setOutputFormatClass(HCatOutputFormat.class);
    HCatOutputFormat.setOutput(job, outputJobInfo);
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
      OutputJobInfo jobInfo = (OutputJobInfo)HCatUtil.deserialize(context.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
      HCatRecord record = new DefaultHCatRecord(3);
      HCatSchema schema = jobInfo.getOutputSchema();
      String vals[] = value.toString().split(",");
      record.setInteger("key",schema,Integer.parseInt(vals[0]));
      for(int i=1;i<vals.length;i++) {
        String pair[] = vals[i].split(":");
        record.set(pair[0],schema,pair[1]);
      }
      context.write(null,record);
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
      for(int i=1;i<vals.length;i++) {
        String pair[] = vals[i].split(":");
        put.add(Bytes.toBytes("my_family"),
            Bytes.toBytes(pair[0]),
            Bytes.toBytes(pair[1]));
      }
      output.collect(null, put);
    }
  }

}
