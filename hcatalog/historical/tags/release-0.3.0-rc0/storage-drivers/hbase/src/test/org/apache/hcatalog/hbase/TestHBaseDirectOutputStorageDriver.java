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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.TableSnapshot;
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test HBaseDirectOuputStorageDriver and HBaseDirectOUtputFormat using a MiniCluster
 */
public class TestHBaseDirectOutputStorageDriver extends SkeletonHBaseTest {

    private final HiveConf allConf;
    private final HCatDriver hcatDriver;

    public TestHBaseDirectOutputStorageDriver() {
        allConf = getHiveConf();
        allConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        allConf.set(HiveConf.ConfVars.HADOOPFS.varname, getFileSystem().getUri().toString());
        allConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new Path(getTestDir(),"warehouse").toString());

        //Add hbase properties
        for (Map.Entry<String, String> el : getHbaseConf())
            allConf.set(el.getKey(), el.getValue());
        for (Map.Entry<String, String> el : getJobConf())
            allConf.set(el.getKey(), el.getValue());

        SessionState.start(new CliSessionState(allConf));
        hcatDriver = new HCatDriver();
    }

    @Test
    public void directOutputFormatTest() throws IOException, ClassNotFoundException, InterruptedException {
        String testName = "directOutputFormatTest";
        Path methodTestDir = new Path(getTestDir(),testName);

        String tableName = newTableName(testName).toLowerCase();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);

        //include hbase config in conf file
        Configuration conf = new Configuration(allConf);
        conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));

        //create table
        createTable(tableName,new String[]{familyName});

        String data[] = {"1,english:ONE,spanish:UNO",
                "2,english:ONE,spanish:DOS",
                "3,english:ONE,spanish:TRES"};



        // input/output settings
        Path inputPath = new Path(methodTestDir,"mr_input");
        getFileSystem().mkdirs(inputPath);
        FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
        for(String line: data)
            os.write(Bytes.toBytes(line + "\n"));
        os.close();

        //create job
        Job job = new Job(conf, testName);
        job.setWorkingDirectory(new Path(methodTestDir,"mr_work"));
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(HBaseDirectOutputFormat.class);
        job.getConfiguration().set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, tableName);

        //manually create transaction
        RevisionManager rm = HBaseHCatStorageHandler.getOpenedRevisionManager(conf);
        try {
            OutputJobInfo outputJobInfo = OutputJobInfo.create("default", tableName, null, null, null);
            Transaction txn = rm.beginWriteTransaction(tableName, Arrays.asList(familyName));
            outputJobInfo.getProperties().setProperty(HBaseConstants.PROPERTY_WRITE_TXN_KEY,
                                                      HCatUtil.serialize(txn));
            job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_INFO,
                                       HCatUtil.serialize(outputJobInfo));
        } finally {
            rm.close();
        }

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(HCatRecord.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(HCatRecord.class);

        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));

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
    public void directOutputStorageDriverTest() throws Exception {
        String testName = "directOutputStorageDriverTest";
        Path methodTestDir = new Path(getTestDir(),testName);

        String databaseName = testName.toLowerCase();
        String dbDir = new Path(methodTestDir,"DB_"+testName).toString();
        String tableName = newTableName(testName).toLowerCase();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);


        //include hbase config in conf file
        Configuration conf = new Configuration(allConf);
        conf.set(HCatConstants.HCAT_KEY_HIVE_CONF, HCatUtil.serialize(allConf.getAllProperties()));


        String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '" + dbDir + "'";
        String tableQuery = "CREATE TABLE " + databaseName + "." + tableName +
                              "(key int, english string, spanish string) STORED BY " +
                              "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'" +
                              "TBLPROPERTIES ('"+HBaseConstants.PROPERTY_OSD_BULK_MODE_KEY+"'='false',"+
                              "'hbase.columns.mapping'=':key,"+familyName+":english,"+familyName+":spanish')" ;

        assertEquals(0, hcatDriver.run(dbquery).getResponseCode());
        assertEquals(0, hcatDriver.run(tableQuery).getResponseCode());

        String data[] = {"1,english:ONE,spanish:UNO",
                               "2,english:ONE,spanish:DOS",
                               "3,english:ONE,spanish:TRES"};

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
        Job job = new Job(conf, testName);
        job.setWorkingDirectory(new Path(methodTestDir,"mr_work"));
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapHCatWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);


        job.setOutputFormatClass(HCatOutputFormat.class);
        OutputJobInfo outputJobInfo = OutputJobInfo.create(databaseName,tableName,null,null,null);
        HCatOutputFormat.setOutput(job,outputJobInfo);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(HCatRecord.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(HCatRecord.class);

        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));

        RevisionManager rm = HBaseHCatStorageHandler.getOpenedRevisionManager(conf);
        try {
            TableSnapshot snapshot = rm.createSnapshot(databaseName+"."+tableName);
            for(String el: snapshot.getColumnFamilies()) {
                assertEquals(1,snapshot.getRevision(el));
            }
        } finally {
            rm.close();
        }

        //verify
        HTable table = new HTable(conf, databaseName+"."+tableName);
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
                assertEquals(1l,result.getColumn(familyNameBytes,Bytes.toBytes(pair[0])).get(0).getTimestamp());
            }
            index++;
        }
        assertEquals(data.length,index);
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

    public static class MapWrite extends Mapper<LongWritable, Text, BytesWritable, Put> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String vals[] = value.toString().split(",");
            Put put = new Put(Bytes.toBytes(vals[0]));
            for(int i=1;i<vals.length;i++) {
                String pair[] = vals[i].split(":");
                put.add(Bytes.toBytes("my_family"),
                        Bytes.toBytes(pair[0]),
                        Bytes.toBytes(pair[1]));
            }
            context.write(new BytesWritable(Bytes.toBytes(vals[0])),put);
        }
    }
}
