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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.HCatDriver;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.pig.HCatLoader;
import org.apache.hcatalog.pig.HCatStorer;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Test;

public class TestPigHBaseStorageHandler extends SkeletonHBaseTest {

	private static HiveConf   hcatConf;
	private static Driver driver;
	private static String mypath;

	private final byte[] FAMILY     = Bytes.toBytes("testFamily");
	private final byte[] QUALIFIER1 = Bytes.toBytes("testQualifier1");
	private final byte[] QUALIFIER2 = Bytes.toBytes("testQualifier2");

	public void Initialize() throws Exception {	    

		hcatConf = new HiveConf(this.getClass());
		//hcatConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
		//		HCatSemanticAnalyzer.class.getName());
		URI fsuri = getFileSystem().getUri();
		Path whPath = new Path(fsuri.getScheme(), fsuri.getAuthority(),
				getTestDir());
		hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
		hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
		hcatConf.set(ConfVars.METASTOREWAREHOUSE.varname, whPath.toString());

		//Add hbase properties
		for (Map.Entry<String, String> el : getHbaseConf()) {
			if (el.getKey().startsWith("hbase.")) {
				hcatConf.set(el.getKey(), el.getValue());
			}
		}

		driver = new Driver(hcatConf);
		SessionState.start(new CliSessionState(hcatConf));

	}

	private void populateHBaseTable(String tName) throws IOException {
		List<Put> myPuts = generatePuts(tName);
		HTable table = new HTable(getHbaseConf(), Bytes.toBytes(tName));
		table.put(myPuts);
	}

	private List<Put> generatePuts(String tableName) throws IOException {

		List<String> columnFamilies = Arrays.asList("testFamily");
		List<Put> myPuts;
        myPuts = new ArrayList<Put>();
        for (int i = 1; i <=10; i++) {
            Put put = new Put(Bytes.toBytes(i));
            put.add(FAMILY, QUALIFIER1, 1, Bytes.toBytes("textA-" + i));
            put.add(FAMILY, QUALIFIER2, 1, Bytes.toBytes("textB-" + i));
            myPuts.add(put);
        }
		return myPuts;
	}
	
	public static void createTestDataFile(String filename) throws IOException {
		FileWriter writer = null;
		int LOOP_SIZE = 10;
                float f = -100.1f;
		try {
			File file = new File(filename);
			file.deleteOnExit();
			writer = new FileWriter(file);
			
			for (int i =1; i <= LOOP_SIZE; i++) {
				writer.write(i+ "\t" +(f+i)+ "\t" + "textB-" + i + "\n");
			}
		} finally {
			if (writer != null) {
				writer.close();
			}
		}

	}

	@Test
	public void testPigHBaseSchema() throws Exception {
		Initialize();

		String tableName = newTableName("MyTable");
		String databaseName = newTableName("MyDatabase");
                //Table name will be lower case unless specified by hbase.table.name property
		String hbaseTableName = "testTable";
		String db_dir = getTestDir() + "/hbasedb";

		String dbQuery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
				+ db_dir + "'";

		String deleteQuery = "DROP TABLE "+databaseName+"."+tableName;

		String tableQuery = "CREATE TABLE " + databaseName + "." + tableName
				+ "(key float, testqualifier1 string, testqualifier2 int) STORED BY " +
				"'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
				+ " WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,testFamily:testQualifier1,testFamily:testQualifier2')" 
				+  " TBLPROPERTIES ('hbase.table.name'='"+hbaseTableName+"')";

		CommandProcessorResponse responseOne = driver.run(deleteQuery);
		assertEquals(0, responseOne.getResponseCode());


		CommandProcessorResponse responseTwo = driver.run(dbQuery);
		assertEquals(0, responseTwo.getResponseCode());


		CommandProcessorResponse responseThree = driver.run(tableQuery);

		HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
		boolean doesTableExist = hAdmin.tableExists(hbaseTableName);
	        assertTrue(doesTableExist);

		PigServer server = new PigServer(ExecType.LOCAL,hcatConf.getAllProperties());
		server.registerQuery("A = load '"+databaseName+"."+tableName+"' using org.apache.hcatalog.pig.HCatLoader();");

		Schema dumpedASchema = server.dumpSchema("A");

		List<FieldSchema> fields = dumpedASchema.getFields();
		assertEquals(3, fields.size());

		assertEquals(DataType.FLOAT,fields.get(0).type);
		assertEquals("key",fields.get(0).alias.toLowerCase());

		assertEquals( DataType.CHARARRAY,fields.get(1).type);
		assertEquals("testQualifier1".toLowerCase(), fields.get(1).alias.toLowerCase());	    

		assertEquals( DataType.INTEGER,fields.get(2).type);
		assertEquals("testQualifier2".toLowerCase(), fields.get(2).alias.toLowerCase());	    

	}		


	@Test
	public void testPigFilterProjection() throws Exception {
		Initialize();

		String tableName = newTableName("MyTable");
		String databaseName = newTableName("MyDatabase");
		//Table name will be lower case unless specified by hbase.table.name property
		String hbaseTableName = (databaseName + "." + tableName).toLowerCase();
		String db_dir = getTestDir() + "/hbasedb";

		String dbQuery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
				+ db_dir + "'";

		String deleteQuery = "DROP TABLE "+databaseName+"."+tableName;

		String tableQuery = "CREATE TABLE " + databaseName + "." + tableName
				+ "(key int, testqualifier1 string, testqualifier2 string) STORED BY " +
				"'org.apache.hadoop.hive.hbase.HBaseStorageHandler'" +
				" WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,testFamily:testQualifier1,testFamily:testQualifier2')" +
                                " TBLPROPERTIES ('hbase.table.default.storage.type'='binary')";

		CommandProcessorResponse responseOne = driver.run(deleteQuery);
		assertEquals(0, responseOne.getResponseCode());


		CommandProcessorResponse responseTwo = driver.run(dbQuery);
		assertEquals(0, responseTwo.getResponseCode());


		CommandProcessorResponse responseThree = driver.run(tableQuery);

		HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
		boolean doesTableExist = hAdmin.tableExists(hbaseTableName);
		assertTrue(doesTableExist);

		populateHBaseTable(hbaseTableName);

		Configuration conf = new Configuration(getHbaseConf());
		HTable table = new HTable(conf, hbaseTableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("testFamily"));
		ResultScanner scanner = table.getScanner(scan);
		int index=1;

		PigServer server = new PigServer(ExecType.LOCAL,hcatConf.getAllProperties());
		server.registerQuery("A = load '"+databaseName+"."+tableName+"' using org.apache.hcatalog.pig.HCatLoader();");
		server.registerQuery("B = filter A by key < 5;");
		server.registerQuery("C = foreach B generate key,testqualifier2;");
		Iterator<Tuple> itr = server.openIterator("C");
                //verify if the filter is correct and returns 2 rows and contains 2 columns and the contents match
		while(itr.hasNext()){
			Tuple t = itr.next();
			assertTrue(t.size() == 2);
			assertTrue(t.get(0).getClass() == Integer.class);
			assertEquals(index,t.get(0));
			assertTrue(t.get(1).getClass() == String.class);
			assertEquals("textB-"+index,t.get(1));
                        index++;
		}
               assertEquals(index-1,4);
	}
	
	@Test
	public void testPigPopulation() throws Exception {
		Initialize();

		String tableName = newTableName("MyTable");
		String databaseName = newTableName("MyDatabase");
		//Table name will be lower case unless specified by hbase.table.name property
		String hbaseTableName = (databaseName + "." + tableName).toLowerCase();
		String db_dir = getTestDir() + "/hbasedb";
		String POPTXT_FILE_NAME = db_dir+"testfile.txt";
                float f = -100.1f;

		String dbQuery = "CREATE DATABASE IF NOT EXISTS " + databaseName + " LOCATION '"
				+ db_dir + "'";

		String deleteQuery = "DROP TABLE "+databaseName+"."+tableName;

		String tableQuery = "CREATE TABLE " + databaseName + "." + tableName
				+ "(key int, testqualifier1 float, testqualifier2 string) STORED BY " +
				"'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
				+ " WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,testFamily:testQualifier1,testFamily:testQualifier2')" 
                                + " TBLPROPERTIES ('hbase.table.default.storage.type'='binary')";

		
		String selectQuery = "SELECT * from "+databaseName.toLowerCase()+"."+tableName.toLowerCase();
		

		CommandProcessorResponse responseOne = driver.run(deleteQuery);
		assertEquals(0, responseOne.getResponseCode());


		CommandProcessorResponse responseTwo = driver.run(dbQuery);
		assertEquals(0, responseTwo.getResponseCode());


		CommandProcessorResponse responseThree = driver.run(tableQuery);

		HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf());
		boolean doesTableExist = hAdmin.tableExists(hbaseTableName);
		assertTrue(doesTableExist);

		
		createTestDataFile(POPTXT_FILE_NAME);

		PigServer server = new PigServer(ExecType.LOCAL,hcatConf.getAllProperties());
		server.registerQuery("A = load '"+POPTXT_FILE_NAME+"' using PigStorage() as (key:int, testqualifier1:float, testqualifier2:chararray);");
		server.registerQuery("B = filter A by (key > 2) AND (key < 8) ;");
		server.registerQuery("store B into '"+databaseName.toLowerCase()+"."+tableName.toLowerCase()+"' using  org.apache.hcatalog.pig.HCatStorer();");
		server.registerQuery("C = load '"+databaseName.toLowerCase()+"."+tableName.toLowerCase()+"' using org.apache.hcatalog.pig.HCatLoader();");
                // Schema should be same
		Schema dumpedBSchema = server.dumpSchema("C");

		List<FieldSchema> fields = dumpedBSchema.getFields();
		assertEquals(3, fields.size());

		assertEquals(DataType.INTEGER,fields.get(0).type);
		assertEquals("key",fields.get(0).alias.toLowerCase());

		assertEquals( DataType.FLOAT,fields.get(1).type);
		assertEquals("testQualifier1".toLowerCase(), fields.get(1).alias.toLowerCase());	    

		assertEquals( DataType.CHARARRAY,fields.get(2).type);
		assertEquals("testQualifier2".toLowerCase(), fields.get(2).alias.toLowerCase());	    
		
		//Query the hbase table and check the key is valid and only 5  are present
		Configuration conf = new Configuration(getHbaseConf());
		HTable table = new HTable(conf, hbaseTableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("testFamily"));
                byte[] familyNameBytes = Bytes.toBytes("testFamily");
		ResultScanner scanner = table.getScanner(scan);
		int index=3;
                int count=0;
		for(Result result: scanner) {
                         //key is correct
			 assertEquals(index,Bytes.toInt(result.getRow()));
                         //first column exists
                         assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes("testQualifier1")));
                         //value is correct
                         assertEquals((index+f),Bytes.toFloat(result.getValue(familyNameBytes,Bytes.toBytes("testQualifier1"))),0);

                         //second column exists
                         assertTrue(result.containsColumn(familyNameBytes,Bytes.toBytes("testQualifier2")));
                         //value is correct
 	                 assertEquals(("textB-"+index).toString(),Bytes.toString(result.getValue(familyNameBytes,Bytes.toBytes("testQualifier2"))));
			index++;
                        count++;
		}
               // 5 rows should be returned
               assertEquals(count,5);

             //Check if hive returns results correctly
	    driver.run(selectQuery);
	    ArrayList<String> result = new ArrayList<String>();
	    driver.getResults(result);
	    //Query using the hive command line
	    assertEquals(5, result.size());
	    Iterator<String> itr = result.iterator();
	    for(int i = 3; i <= 7; i++) {
	    	String tokens[] = itr.next().split("\\s+");
	        assertEquals(i,Integer.parseInt(tokens[0]));
	        assertEquals(i+f,Float.parseFloat(tokens[1]),0);
	        assertEquals(("textB-"+i).toString(),tokens[2]);
	    }
	    
	    //delete the table from the database
	    CommandProcessorResponse responseFour = driver.run(deleteQuery);
		assertEquals(0, responseFour.getResponseCode());

	}

}
