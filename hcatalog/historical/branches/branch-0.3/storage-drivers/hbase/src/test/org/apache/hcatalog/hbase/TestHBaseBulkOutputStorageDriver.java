package org.apache.hcatalog.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests components of HBaseBulkOutputStorageDriver using ManyMiniCluster.
 * Including ImprtSequenceFile, HBaseOutputStorageDrivers and HBaseBulkOutputFormat
 */
public class TestHBaseBulkOutputStorageDriver extends SkeletonHBaseTest {
    private final static Log LOG = LogFactory.getLog(TestHBaseBulkOutputStorageDriver.class);

    private final HiveConf allConf;
    private final HCatDriver hcatDriver;

    public TestHBaseBulkOutputStorageDriver() {
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

    public static class MapWrite extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

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
            context.write(new ImmutableBytesWritable(Bytes.toBytes(vals[0])),put);
        }
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

    @Test
    public void hbaseBulkOutputFormatTest() throws IOException, ClassNotFoundException, InterruptedException {
        String testName = "hbaseBulkOutputFormatTest";
        Path methodTestDir = new Path(getTestDir(),testName);
        LOG.info("starting: "+testName);

        String tableName = newTableName(testName).toLowerCase();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);

        //include hbase config in conf file
        Configuration conf = new Configuration(allConf);

        //create table
        conf.set(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY, tableName);
        createTable(tableName, new String[]{familyName});

        String data[] = {"1,english:one,spanish:uno",
                               "2,english:two,spanish:dos",
                               "3,english:three,spanish:tres"};



        // input/output settings
        Path inputPath = new Path(methodTestDir,"mr_input");
        FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
        for(String line: data)
            os.write(Bytes.toBytes(line + "\n"));
        os.close();
        Path interPath = new Path(methodTestDir,"inter");
        //create job
        Job job = new Job(conf, testName);
        HBaseHCatStorageHandler.addDependencyJars(job.getConfiguration());
        job.setWorkingDirectory(new Path(methodTestDir,"mr_work"));
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(HBaseBulkOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,interPath);

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

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(HCatRecord.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
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
        //test if load count is the same
        assertEquals(data.length,index);
        //test if scratch directory was erased
        assertFalse(FileSystem.get(job.getConfiguration()).exists(interPath));
    }

    @Test
    public void importSequenceFileTest() throws IOException, ClassNotFoundException, InterruptedException {
        String testName = "importSequenceFileTest";
        Path methodTestDir = new Path(getTestDir(),testName);
        LOG.info("starting: "+testName);

        String tableName = newTableName(testName).toLowerCase();
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String familyName = "my_family";
        byte[] familyNameBytes = Bytes.toBytes(familyName);

        //include hbase config in conf file
        Configuration conf = new Configuration(allConf);

        //create table
        createTable(tableName,new String[]{familyName});

        String data[] = {"1,english:one,spanish:uno",
                               "2,english:two,spanish:dos",
                               "3,english:three,spanish:tres"};



        // input/output settings
        Path inputPath = new Path(methodTestDir,"mr_input");
        getFileSystem().mkdirs(inputPath);
        FSDataOutputStream os = getFileSystem().create(new Path(inputPath,"inputFile.txt"));
        for(String line: data)
            os.write(Bytes.toBytes(line + "\n"));
        os.close();
        Path interPath = new Path(methodTestDir,"inter");
        Path scratchPath = new Path(methodTestDir,"scratch");


        //create job
        Job job = new Job(conf, testName);
        HBaseHCatStorageHandler.addDependencyJars(job.getConfiguration());
        job.setWorkingDirectory(new Path(methodTestDir,"mr_work"));
        job.setJarByClass(this.getClass());
        job.setMapperClass(MapWrite.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,interPath);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        assertTrue(job.waitForCompletion(true));

        job = new Job(new Configuration(allConf),testName+"_importer");
        HBaseHCatStorageHandler.addDependencyJars(job.getConfiguration());
        assertTrue(ImportSequenceFile.runJob(job, tableName, interPath, scratchPath));

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
        //test if load count is the same
        assertEquals(data.length,index);
        //test if scratch directory was erased
        assertFalse(FileSystem.get(job.getConfiguration()).exists(scratchPath));
    }

    @Test
    public void hbaseBulkOutputStorageDriverTest() throws Exception {
        String testName = "hbaseBulkOutputStorageDriverTest";
        Path methodTestDir = new Path(getTestDir(),testName);
        LOG.info("starting: "+testName);

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
                              "TBLPROPERTIES ('hbase.columns.mapping'=':key,"+familyName+":english,"+familyName+":spanish')" ;

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
        Job job = new Job(conf,testName);
        HBaseHCatStorageHandler.addDependencyJars(job.getConfiguration());
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
        //test if load count is the same
        assertEquals(data.length,index);
    }

    @Test
    public void hbaseBulkOutputStorageDriverTestWithDefaultDB() throws Exception {
        String testName = "hbaseBulkOutputStorageDriverTestWithDefaultDB";
        Path methodTestDir = new Path(getTestDir(),testName);

        String databaseName = "default";
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
                              "TBLPROPERTIES (" +
                              "'hbase.columns.mapping'=':key,"+familyName+":english,"+familyName+":spanish')" ;

        assertEquals(0, hcatDriver.run(dbquery).getResponseCode());
        assertEquals(0, hcatDriver.run(tableQuery).getResponseCode());

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
        Job job = new Job(conf,testName);
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
        //test if load count is the same
        assertEquals(data.length,index);
    }

}

