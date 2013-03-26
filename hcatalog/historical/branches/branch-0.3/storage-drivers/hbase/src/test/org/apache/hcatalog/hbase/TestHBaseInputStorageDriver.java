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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.hcatalog.hbase.snapshot.Transaction;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.junit.Test;

public class TestHBaseInputStorageDriver extends SkeletonHBaseTest {

    private static HiveConf   hcatConf;
    private static HCatDriver hcatDriver;
    private final byte[] FAMILY     = Bytes.toBytes("testFamily");
    private final byte[] QUALIFIER1 = Bytes.toBytes("testQualifier1");
    private final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");

    private List<Put> generatePuts(int num, String tableName) throws IOException {

        List<String> columnFamilies = Arrays.asList("testFamily");
        RevisionManager rm = null;
        List<Put> myPuts;
        try {
            rm = HBaseHCatStorageHandler
                    .getOpenedRevisionManager(getHbaseConf());
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

   private void Initialize() throws Exception {
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

        SessionState.start(new CliSessionState(hcatConf));
        hcatDriver = new HCatDriver();

    }

   private void populateHBaseTable(String tName, int revisions) throws IOException {
        List<Put> myPuts = generatePuts(revisions, tName);
        HTable table = new HTable(getHbaseConf(), Bytes.toBytes(tName));
        table.put(myPuts);
    }

    @Test
    public void TestHBaseTableReadMR() throws Exception {
        Initialize();
        String tableName = newTableName("mytable");
        String databaseName = newTableName("mydatabase");
        String db_dir = getTestDir() + "/hbasedb";

        String dbquery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
                            + db_dir + "'";
        String tableQuery = "CREATE TABLE " + databaseName + "." + tableName
                              + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
                              "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
                              + "TBLPROPERTIES ('hbase.columns.mapping'=':key,testFamily:testQualifier1,testFamily:testQualifier2')" ;

        CommandProcessorResponse responseOne = hcatDriver.run(dbquery);
        assertEquals(0, responseOne.getResponseCode());
        CommandProcessorResponse responseTwo = hcatDriver.run(tableQuery);
        assertEquals(0, responseTwo.getResponseCode());

        HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
        String hbaseTableName = databaseName + "." + tableName;
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

        job.setInputFormatClass(HCatInputFormat.class);
        InputJobInfo inputJobInfo = InputJobInfo.create(databaseName, tableName,
                null, null, null);
        HCatInputFormat.setInput(job, inputJobInfo);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));
        assertFalse(MapReadHTable.error);
        assertEquals(MapReadHTable.count, 1);

        String dropTableQuery = "DROP TABLE " + hbaseTableName ;
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

        Initialize();
        String tableName = newTableName("mytable");
        String tableQuery = "CREATE TABLE " + tableName
                              + "(key string, testqualifier1 string, testqualifier2 string) STORED BY " +
                              "'org.apache.hcatalog.hbase.HBaseHCatStorageHandler'"
                              + "TBLPROPERTIES ('hbase.columns.mapping'=':key," +
                              		"testFamily:testQualifier1,testFamily:testQualifier2')" ;

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
        InputJobInfo inputJobInfo = InputJobInfo.create(
                MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName, null, null,
                null);
        HCatInputFormat.setOutputSchema(job, getProjectionSchema());
        HCatInputFormat.setInput(job, inputJobInfo);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        assertTrue(job.waitForCompletion(true));
        assertFalse(MapReadProjHTable.error);
        assertEquals(MapReadProjHTable.count, 1);

        String dropTableQuery = "DROP TABLE " + tableName ;
        CommandProcessorResponse responseThree = hcatDriver.run(dropTableQuery);
        assertEquals(0, responseThree.getResponseCode());

        boolean isHbaseTableThere = hAdmin.tableExists(tableName);
        assertFalse(isHbaseTableThere);
    }


    static class MapReadHTable
            extends
            Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable, Text> {

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
    }

    static class MapReadProjHTable
            extends
            Mapper<ImmutableBytesWritable, HCatRecord, WritableComparable, Text> {

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

 private HCatSchema getProjectionSchema() throws HCatException {

        HCatSchema schema = new HCatSchema(new ArrayList<HCatFieldSchema>());
        schema.append(new HCatFieldSchema("key", HCatFieldSchema.Type.STRING,
                ""));
        schema.append(new HCatFieldSchema("testqualifier1",
                HCatFieldSchema.Type.STRING, ""));
        return schema;
    }


}
