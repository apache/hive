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
package org.apache.hive.hcatalog.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.Pair;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseTest;
import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHCatLoaderTest extends HCatBaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHCatLoaderTest.class);
  private static final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private static final String COMPLEX_FILE_NAME = TEST_DATA_DIR + "/complex.input.data";
  private static final String DATE_FILE_NAME = TEST_DATA_DIR + "/datetimestamp.input.data";

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String COMPLEX_TABLE = "junit_unparted_complex";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  private static final String SPECIFIC_SIZE_TABLE = "junit_specific_size";
  private static final String PARTITIONED_DATE_TABLE = "junit_parted_date";

  private Map<Integer, Pair<Integer, String>> basicInputData;

  protected String storageFormat;

  abstract String getStorageFormat();

  public AbstractHCatLoaderTest() {
    this.storageFormat = getStorageFormat();
  }

  private void dropTable(String tablename) throws IOException, CommandNeedRetryException {
    dropTable(tablename, driver);
  }

  static void dropTable(String tablename, Driver driver) throws IOException, CommandNeedRetryException {
    driver.run("drop table if exists " + tablename);
  }

  private void createTable(String tablename, String schema, String partitionedBy) throws IOException, CommandNeedRetryException {
    createTable(tablename, schema, partitionedBy, driver, storageFormat);
  }

  static void createTable(String tablename, String schema, String partitionedBy, Driver driver, String storageFormat)
      throws IOException, CommandNeedRetryException {
    String createTable;
    createTable = "create table " + tablename + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      createTable = createTable + "partitioned by (" + partitionedBy + ") ";
    }
    createTable = createTable + "stored as " +storageFormat;
    executeStatementOnDriver(createTable, driver);
  }

  private void createTable(String tablename, String schema) throws IOException, CommandNeedRetryException {
    createTable(tablename, schema, null);
  }

  /**
   * Execute Hive CLI statement
   * @param cmd arbitrary statement to execute
   */
  static void executeStatementOnDriver(String cmd, Driver driver) throws IOException, CommandNeedRetryException {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if(cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". Driver returned " + cpr.getResponseCode() + " Error: " + cpr.getErrorMessage());
    }
  }

  private static void checkProjection(FieldSchema fs, String expectedName, byte expectedPigType) {
    assertEquals(fs.alias, expectedName);
    assertEquals("Expected " + DataType.findTypeName(expectedPigType) + "; got " +
      DataType.findTypeName(fs.type), expectedPigType, fs.type);
  }

  @Before
  public void setUpTest() throws Exception {
    createTable(BASIC_TABLE, "a int, b string");
    createTable(COMPLEX_TABLE,
      "name string, studentid int, "
        + "contact struct<phno:string,email:string>, "
        + "currently_registered_courses array<string>, "
        + "current_grades map<string,string>, "
        + "phnos array<struct<phno:string,type:string>>");

    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");
    createTable(SPECIFIC_SIZE_TABLE, "a int, b string");
    createTable(PARTITIONED_DATE_TABLE, "b string", "dt date");
    AllTypesTable.setupAllTypesTable(driver);

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    basicInputData = new HashMap<Integer, Pair<Integer, String>>();
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input[k] = si + "\t" + sj;
        basicInputData.put(k, new Pair<Integer, String>(i, sj));
        k++;
      }
    }
    HcatTestUtils.createTestDataFile(BASIC_FILE_NAME, input);
    HcatTestUtils.createTestDataFile(COMPLEX_FILE_NAME,
      new String[]{
        "Henry Jekyll\t42\t(415-253-6367,hjekyll@contemporary.edu.uk)\t{(PHARMACOLOGY),(PSYCHIATRY)}\t[PHARMACOLOGY#A-,PSYCHIATRY#B+]\t{(415-253-6367,cell),(408-253-6367,landline)}",
        "Edward Hyde\t1337\t(415-253-6367,anonymous@b44chan.org)\t{(CREATIVE_WRITING),(COPYRIGHT_LAW)}\t[CREATIVE_WRITING#A+,COPYRIGHT_LAW#D]\t{(415-253-6367,cell),(408-253-6367,landline)}",
      }
    );
    HcatTestUtils.createTestDataFile(DATE_FILE_NAME,
      new String[]{
        "2016-07-14 08:10:15\tHenry Jekyll",
        "2016-07-15 11:54:55\tEdward Hyde",
      }
    );
    PigServer server = createPigServer(false);
    server.setBatchOn();
    int i = 0;
    server.registerQuery("A = load '" + BASIC_FILE_NAME + "' as (a:int, b:chararray);", ++i);

    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.registerQuery("store A into '" + SPECIFIC_SIZE_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.registerQuery("B = foreach A generate a,b;", ++i);
    server.registerQuery("B2 = filter B by a < 2;", ++i);
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=0');", ++i);

    server.registerQuery("C = foreach A generate a,b;", ++i);
    server.registerQuery("C2 = filter C by a >= 2;", ++i);
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer('bkt=1');", ++i);

    server.registerQuery("D = load '" + COMPLEX_FILE_NAME + "' as (name:chararray, studentid:int, contact:tuple(phno:chararray,email:chararray), currently_registered_courses:bag{innertup:tuple(course:chararray)}, current_grades:map[ ] , phnos :bag{innertup:tuple(phno:chararray,type:chararray)});", ++i);
    server.registerQuery("store D into '" + COMPLEX_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();", ++i);

    server.registerQuery("E = load '" + DATE_FILE_NAME + "' as (dt:chararray, b:chararray);", ++i);
    server.registerQuery("F = foreach E generate ToDate(dt, 'yyyy-MM-dd HH:mm:ss') as dt, b;", ++i);
    server.registerQuery("store F into '" + PARTITIONED_DATE_TABLE + "' using org.apache.hive.hcatalog.pig.HCatStorer();", ++i);

    server.executeBatch();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (driver != null) {
        dropTable(BASIC_TABLE);
        dropTable(COMPLEX_TABLE);
        dropTable(PARTITIONED_TABLE);
        dropTable(SPECIFIC_SIZE_TABLE);
        dropTable(PARTITIONED_DATE_TABLE);
        dropTable(AllTypesTable.ALL_PRIMITIVE_TYPES_TABLE);
      }
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }

  @Test
  public void testSchemaLoadBasic() throws IOException {
    PigServer server = createPigServer(false);

    // test that schema was loaded correctly
    server.registerQuery("X = load '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Schema dumpedXSchema = server.dumpSchema("X");
    List<FieldSchema> Xfields = dumpedXSchema.getFields();
    assertEquals(2, Xfields.size());
    assertTrue(Xfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Xfields.get(0).type == DataType.INTEGER);
    assertTrue(Xfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Xfields.get(1).type == DataType.CHARARRAY);

  }

  /**
   * Test that we properly translate data types in Hive/HCat table schema into Pig schema
   */
  @Test
  public void testSchemaLoadPrimitiveTypes() throws IOException {
    AllTypesTable.testSchemaLoadPrimitiveTypes();
  }

  /**
   * Test that value from Hive table are read properly in Pig
   */
  @Test
  public void testReadDataPrimitiveTypes() throws Exception {
    AllTypesTable.testReadDataPrimitiveTypes();
  }

  @Test
  public void testReadDataBasic() throws IOException {
    PigServer server = createPigServer(false);

    server.registerQuery("X = load '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> XIter = server.openIterator("X");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(2, t.size());
      assertNotNull(t.get(0));
      assertNotNull(t.get(1));
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      assertEquals(t.get(0), basicInputData.get(numTuplesRead).first);
      assertEquals(t.get(1), basicInputData.get(numTuplesRead).second);
      numTuplesRead++;
    }
    assertEquals(basicInputData.size(), numTuplesRead);
  }

  @Test
  public void testSchemaLoadComplex() throws IOException {
    PigServer server = createPigServer(false);

    // test that schema was loaded correctly
    server.registerQuery("K = load '" + COMPLEX_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Schema dumpedKSchema = server.dumpSchema("K");
    List<FieldSchema> Kfields = dumpedKSchema.getFields();
    assertEquals(6, Kfields.size());

    assertEquals(DataType.CHARARRAY, Kfields.get(0).type);
    assertEquals("name", Kfields.get(0).alias.toLowerCase());

    assertEquals(DataType.INTEGER, Kfields.get(1).type);
    assertEquals("studentid", Kfields.get(1).alias.toLowerCase());

    assertEquals(DataType.TUPLE, Kfields.get(2).type);
    assertEquals("contact", Kfields.get(2).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(2).schema);
      assertTrue(Kfields.get(2).schema.getFields().size() == 2);
      assertTrue(Kfields.get(2).schema.getFields().get(0).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(0).alias.equalsIgnoreCase("phno"));
      assertTrue(Kfields.get(2).schema.getFields().get(1).type == DataType.CHARARRAY);
      assertTrue(Kfields.get(2).schema.getFields().get(1).alias.equalsIgnoreCase("email"));
    }
    assertEquals(DataType.BAG, Kfields.get(3).type);
    assertEquals("currently_registered_courses", Kfields.get(3).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(3).schema);
      assertEquals(1, Kfields.get(3).schema.getFields().size());
      assertEquals(DataType.TUPLE, Kfields.get(3).schema.getFields().get(0).type);
      assertNotNull(Kfields.get(3).schema.getFields().get(0).schema);
      assertEquals(1, Kfields.get(3).schema.getFields().get(0).schema.getFields().size());
      assertEquals(DataType.CHARARRAY, Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).type);
      // assertEquals("course",Kfields.get(3).schema.getFields().get(0).schema.getFields().get(0).alias.toLowerCase());
      // commented out, because the name becomes "innerfield" by default - we call it "course" in pig,
      // but in the metadata, it'd be anonymous, so this would be autogenerated, which is fine
    }
    assertEquals(DataType.MAP, Kfields.get(4).type);
    assertEquals("current_grades", Kfields.get(4).alias.toLowerCase());
    assertEquals(DataType.BAG, Kfields.get(5).type);
    assertEquals("phnos", Kfields.get(5).alias.toLowerCase());
    {
      assertNotNull(Kfields.get(5).schema);
      assertEquals(1, Kfields.get(5).schema.getFields().size());
      assertEquals(DataType.TUPLE, Kfields.get(5).schema.getFields().get(0).type);
      assertNotNull(Kfields.get(5).schema.getFields().get(0).schema);
      assertTrue(Kfields.get(5).schema.getFields().get(0).schema.getFields().size() == 2);
      assertEquals(DataType.CHARARRAY, Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).type);
      assertEquals("phno", Kfields.get(5).schema.getFields().get(0).schema.getFields().get(0).alias.toLowerCase());
      assertEquals(DataType.CHARARRAY, Kfields.get(5).schema.getFields().get(0).schema.getFields().get(1).type);
      assertEquals("type", Kfields.get(5).schema.getFields().get(0).schema.getFields().get(1).alias.toLowerCase());
    }

  }

  @Test
  public void testReadPartitionedBasic() throws IOException, CommandNeedRetryException {
    PigServer server = createPigServer(false);

    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), valuesReadFromHiveDriver.size());

    server.registerQuery("W = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Schema dumpedWSchema = server.dumpSchema("W");
    List<FieldSchema> Wfields = dumpedWSchema.getFields();
    assertEquals(3, Wfields.size());
    assertTrue(Wfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Wfields.get(0).type == DataType.INTEGER);
    assertTrue(Wfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Wfields.get(1).type == DataType.CHARARRAY);
    assertTrue(Wfields.get(2).alias.equalsIgnoreCase("bkt"));
    assertTrue(Wfields.get(2).type == DataType.CHARARRAY);

    Iterator<Tuple> WIter = server.openIterator("W");
    Collection<Pair<Integer, String>> valuesRead = new ArrayList<Pair<Integer, String>>();
    while (WIter.hasNext()) {
      Tuple t = WIter.next();
      assertTrue(t.size() == 3);
      assertNotNull(t.get(0));
      assertNotNull(t.get(1));
      assertNotNull(t.get(2));
      assertTrue(t.get(0).getClass() == Integer.class);
      assertTrue(t.get(1).getClass() == String.class);
      assertTrue(t.get(2).getClass() == String.class);
      valuesRead.add(new Pair<Integer, String>((Integer) t.get(0), (String) t.get(1)));
      if ((Integer) t.get(0) < 2) {
        assertEquals("0", t.get(2));
      } else {
        assertEquals("1", t.get(2));
      }
    }
    assertEquals(valuesReadFromHiveDriver.size(), valuesRead.size());

    server.registerQuery("P1 = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    server.registerQuery("P1filter = filter P1 by bkt == '0';");
    Iterator<Tuple> P1Iter = server.openIterator("P1filter");
    int count1 = 0;
    while (P1Iter.hasNext()) {
      Tuple t = P1Iter.next();

      assertEquals("0", t.get(2));
      assertEquals(1, t.get(0));
      count1++;
    }
    assertEquals(3, count1);

    server.registerQuery("P2 = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    server.registerQuery("P2filter = filter P2 by bkt == '1';");
    Iterator<Tuple> P2Iter = server.openIterator("P2filter");
    int count2 = 0;
    while (P2Iter.hasNext()) {
      Tuple t = P2Iter.next();

      assertEquals("1", t.get(2));
      assertTrue(((Integer) t.get(0)) > 1);
      count2++;
    }
    assertEquals(6, count2);
  }

  @Test
  public void testReadMissingPartitionBasicNeg() throws IOException, CommandNeedRetryException {
    PigServer server = createPigServer(false);

    File removedPartitionDir = new File(TEST_WAREHOUSE_DIR + "/" + PARTITIONED_TABLE + "/bkt=0");
    if (!removeDirectory(removedPartitionDir)) {
      System.out.println("Test did not run because its environment could not be set.");
      return;
    }
    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    assertTrue(valuesReadFromHiveDriver.size() == 6);

    server.registerQuery("W = load '" + PARTITIONED_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Schema dumpedWSchema = server.dumpSchema("W");
    List<FieldSchema> Wfields = dumpedWSchema.getFields();
    assertEquals(3, Wfields.size());
    assertTrue(Wfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Wfields.get(0).type == DataType.INTEGER);
    assertTrue(Wfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Wfields.get(1).type == DataType.CHARARRAY);
    assertTrue(Wfields.get(2).alias.equalsIgnoreCase("bkt"));
    assertTrue(Wfields.get(2).type == DataType.CHARARRAY);

    try {
      Iterator<Tuple> WIter = server.openIterator("W");
      fail("Should failed in retriving an invalid partition");
    } catch (IOException ioe) {
      // expected
    }
  }

  private static boolean removeDirectory(File dir) {
    boolean success = false;
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null && files.length > 0) {
        for (File file : files) {
          success = removeDirectory(file);
          if (!success) {
            return false;
          }
        }
      }
      success = dir.delete();
    } else {
        success = dir.delete();
    }
    return success;
  }

  @Test
  public void testProjectionsBasic() throws IOException {
    PigServer server = createPigServer(false);

    // projections are handled by using generate, not "as" on the Load

    server.registerQuery("Y1 = load '" + BASIC_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    server.registerQuery("Y2 = foreach Y1 generate a;");
    server.registerQuery("Y3 = foreach Y1 generate b,a;");
    Schema dumpedY2Schema = server.dumpSchema("Y2");
    Schema dumpedY3Schema = server.dumpSchema("Y3");
    List<FieldSchema> Y2fields = dumpedY2Schema.getFields();
    List<FieldSchema> Y3fields = dumpedY3Schema.getFields();
    assertEquals(1, Y2fields.size());
    assertEquals("a", Y2fields.get(0).alias.toLowerCase());
    assertEquals(DataType.INTEGER, Y2fields.get(0).type);
    assertEquals(2, Y3fields.size());
    assertEquals("b", Y3fields.get(0).alias.toLowerCase());
    assertEquals(DataType.CHARARRAY, Y3fields.get(0).type);
    assertEquals("a", Y3fields.get(1).alias.toLowerCase());
    assertEquals(DataType.INTEGER, Y3fields.get(1).type);

    int numTuplesRead = 0;
    Iterator<Tuple> Y2Iter = server.openIterator("Y2");
    while (Y2Iter.hasNext()) {
      Tuple t = Y2Iter.next();
      assertEquals(t.size(), 1);
      assertNotNull(t.get(0));
      assertTrue(t.get(0).getClass() == Integer.class);
      assertEquals(t.get(0), basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    numTuplesRead = 0;
    Iterator<Tuple> Y3Iter = server.openIterator("Y3");
    while (Y3Iter.hasNext()) {
      Tuple t = Y3Iter.next();
      assertEquals(t.size(), 2);
      assertNotNull(t.get(0));
      assertTrue(t.get(0).getClass() == String.class);
      assertEquals(t.get(0), basicInputData.get(numTuplesRead).second);
      assertNotNull(t.get(1));
      assertTrue(t.get(1).getClass() == Integer.class);
      assertEquals(t.get(1), basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    assertEquals(basicInputData.size(), numTuplesRead);
  }

  @Test
  public void testColumnarStorePushdown() throws Exception {
    String PIGOUTPUT_DIR = TEST_DATA_DIR+ "/colpushdownop";
    String PIG_FILE = "test.pig";
    String expectedCols = "0,1";
    PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
    w.println("A = load '" + COMPLEX_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    w.println("B = foreach A generate name,studentid;");
    w.println("C = filter B by name is not null;");
    w.println("store C into '" + PIGOUTPUT_DIR + "' using PigStorage();");
    w.close();

    try {
      String[] args = { "-x", "local", PIG_FILE };
      PigStats stats = PigRunner.run(args, null);
      //Pig script was successful
      assertTrue(stats.isSuccessful());
      //Single MapReduce job is launched
      OutputStats outstats = stats.getOutputStats().get(0);
      assertTrue(outstats!= null);
      assertEquals(expectedCols,outstats.getConf()
        .get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
      //delete output file on exit
      FileSystem fs = FileSystem.get(outstats.getConf());
      if (fs.exists(new Path(PIGOUTPUT_DIR))) {
        fs.delete(new Path(PIGOUTPUT_DIR), true);
      }
    }finally {
      new File(PIG_FILE).delete();
    }
  }

  /**
   * Tests the failure case caused by HIVE-10752
   * @throws Exception
   */
  @Test
  public void testColumnarStorePushdown2() throws Exception {
    PigServer server = createPigServer(false);
    server.registerQuery("A = load '" + COMPLEX_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    server.registerQuery("B = load '" + COMPLEX_TABLE + "' using org.apache.hive.hcatalog.pig.HCatLoader();");
    server.registerQuery("C = join A by name, B by name;");
    server.registerQuery("D = foreach C generate B::studentid;");
    server.registerQuery("E = ORDER D by studentid asc;");

    Iterator<Tuple> iter = server.openIterator("E");
    Tuple t = iter.next();
    assertEquals(42, t.get(0));

    t = iter.next();
    assertEquals(1337, t.get(0));
  }

  @Test
  public void testGetInputBytes() throws Exception {
    File file = new File(TEST_WAREHOUSE_DIR + "/" + SPECIFIC_SIZE_TABLE + "/part-m-00000");
    file.deleteOnExit();
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
    randomAccessFile.setLength(2L * 1024 * 1024 * 1024);
    randomAccessFile.close();
    Job job = new Job();
    HCatLoader hCatLoader = new HCatLoader();
    hCatLoader.setUDFContextSignature("testGetInputBytes");
    hCatLoader.setLocation(SPECIFIC_SIZE_TABLE, job);
    ResourceStatistics statistics = hCatLoader.getStatistics(file.getAbsolutePath(), job);
    assertEquals(2048, (long) statistics.getmBytes());
  }

  @Test
  public void testConvertBooleanToInt() throws Exception {
    String tbl = "test_convert_boolean_to_int";
    String inputFileName = TEST_DATA_DIR + "/testConvertBooleanToInt/data.txt";
    File inputDataDir = new File(inputFileName).getParentFile();
    inputDataDir.mkdir();

    String[] lines = new String[]{"llama\ttrue", "alpaca\tfalse"};
    HcatTestUtils.createTestDataFile(inputFileName, lines);

    assertEquals(0, driver.run("drop table if exists " + tbl).getResponseCode());
    assertEquals(0, driver.run("create external table " + tbl +
      " (a string, b boolean) row format delimited fields terminated by '\t'" +
      " stored as textfile location 'file:///" +
      inputDataDir.getPath().replaceAll("\\\\", "/") + "'").getResponseCode());

    Properties properties = new Properties();
    properties.setProperty(HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER, "true");
    properties.put("stop.on.failure", Boolean.TRUE.toString());
    PigServer server = new PigServer(ExecType.LOCAL, properties);
    server.registerQuery(
      "data = load 'test_convert_boolean_to_int' using org.apache.hive.hcatalog.pig.HCatLoader();");
    Schema schema = server.dumpSchema("data");
    assertEquals(2, schema.getFields().size());

    assertEquals("a", schema.getField(0).alias);
    assertEquals(DataType.CHARARRAY, schema.getField(0).type);
    assertEquals("b", schema.getField(1).alias);
    if (PigHCatUtil.pigHasBooleanSupport()){
      assertEquals(DataType.BOOLEAN, schema.getField(1).type);
    } else {
      assertEquals(DataType.INTEGER, schema.getField(1).type);
    }

    Iterator<Tuple> iterator = server.openIterator("data");
    Tuple t = iterator.next();
    assertEquals("llama", t.get(0));
    assertEquals(1, t.get(1));
    t = iterator.next();
    assertEquals("alpaca", t.get(0));
    assertEquals(0, t.get(1));
    assertFalse(iterator.hasNext());
  }

  /**
   * Test if we can read a date partitioned table
   */
  @Test
  public void testDatePartitionPushUp() throws Exception {
    PigServer server = createPigServer(false);
    server.registerQuery("X = load '" + PARTITIONED_DATE_TABLE + "' using " + HCatLoader.class.getName() + "();");
    server.registerQuery("Y = filter X by dt == ToDate('2016-07-14','yyyy-MM-dd');");
    Iterator<Tuple> YIter = server.openIterator("Y");
    int numTuplesRead = 0;
    while (YIter.hasNext()) {
      Tuple t = YIter.next();
      assertEquals(t.size(), 2);
      numTuplesRead++;
    }
    assertTrue("Expected " + 1 + "; found " + numTuplesRead, numTuplesRead == 1);
  }

  /**
   * basic tests that cover each scalar type
   * https://issues.apache.org/jira/browse/HIVE-5814
   */
  protected static final class AllTypesTable {
    private static final String ALL_TYPES_FILE_NAME = TEST_DATA_DIR + "/alltypes.input.data";
    private static final String ALL_PRIMITIVE_TYPES_TABLE = "junit_unparted_alltypes";
    private static final String ALL_TYPES_SCHEMA = "( c_boolean boolean, " +   //0
        "c_tinyint tinyint, " +     //1
        "c_smallint smallint, " +   //2
        "c_int int, " +             //3
        "c_bigint bigint, " +       //4
        "c_float float, " +         //5
        "c_double double, " +       //6
        "c_decimal decimal(5,2), " +//7
        "c_string string, " +       //8
        "c_char char(10), " +       //9
        "c_varchar varchar(20), " + //10
        "c_binary binary, " +       //11
        "c_date date, " +           //12
        "c_timestamp timestamp)";   //13
    /**
     * raw data for #ALL_PRIMITIVE_TYPES_TABLE
     * All the values are within range of target data type (column)
     */
    private static final Object[][] primitiveRows = new Object[][] {
        {Boolean.TRUE,Byte.MAX_VALUE,Short.MAX_VALUE, Integer.MAX_VALUE,Long.MAX_VALUE,Float.MAX_VALUE,Double.MAX_VALUE,555.22,"Kyiv","char(10)xx","varchar(20)","blah".getBytes(),Date.valueOf("2014-01-13"),Timestamp.valueOf("2014-01-13 19:26:25.0123")},
        {Boolean.FALSE,Byte.MIN_VALUE,Short.MIN_VALUE, Integer.MIN_VALUE,Long.MIN_VALUE,Float.MIN_VALUE,Double.MIN_VALUE,-555.22,"Saint Petersburg","char(xx)00","varchar(yy)","doh".getBytes(),Date.valueOf("2014-01-14"), Timestamp.valueOf("2014-01-14 19:26:25.0123")}
    };
    /**
     * Test that we properly translate data types in Hive/HCat table schema into Pig schema
     */
    static void testSchemaLoadPrimitiveTypes() throws IOException {
      PigServer server = createPigServer(false);
      server.registerQuery("X = load '" + ALL_PRIMITIVE_TYPES_TABLE + "' using " + HCatLoader.class.getName() + "();");
      Schema dumpedXSchema = server.dumpSchema("X");
      List<FieldSchema> Xfields = dumpedXSchema.getFields();
      assertEquals("Expected " + HCatFieldSchema.Type.numPrimitiveTypes() + " fields, found " +
          Xfields.size(), HCatFieldSchema.Type.numPrimitiveTypes(), Xfields.size());
      checkProjection(Xfields.get(0), "c_boolean", DataType.BOOLEAN);
      checkProjection(Xfields.get(1), "c_tinyint", DataType.INTEGER);
      checkProjection(Xfields.get(2), "c_smallint", DataType.INTEGER);
      checkProjection(Xfields.get(3), "c_int", DataType.INTEGER);
      checkProjection(Xfields.get(4), "c_bigint", DataType.LONG);
      checkProjection(Xfields.get(5), "c_float", DataType.FLOAT);
      checkProjection(Xfields.get(6), "c_double", DataType.DOUBLE);
      checkProjection(Xfields.get(7), "c_decimal", DataType.BIGDECIMAL);
      checkProjection(Xfields.get(8), "c_string", DataType.CHARARRAY);
      checkProjection(Xfields.get(9), "c_char", DataType.CHARARRAY);
      checkProjection(Xfields.get(10), "c_varchar", DataType.CHARARRAY);
      checkProjection(Xfields.get(11), "c_binary", DataType.BYTEARRAY);
      checkProjection(Xfields.get(12), "c_date", DataType.DATETIME);
      checkProjection(Xfields.get(13), "c_timestamp", DataType.DATETIME);
    }
    /**
     * Test that value from Hive table are read properly in Pig
     */
    private static void testReadDataPrimitiveTypes() throws Exception {
      // testConvertBooleanToInt() sets HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER=true, and
      // might be the last one to call HCatContext.INSTANCE.setConf(). Make sure setting is false.
      Properties properties = new Properties();
      properties.setProperty(HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER, "false");
      PigServer server = new PigServer(ExecType.LOCAL, properties);
      server.registerQuery("X = load '" + ALL_PRIMITIVE_TYPES_TABLE + "' using " + HCatLoader.class.getName() + "();");
      Iterator<Tuple> XIter = server.openIterator("X");
      int numTuplesRead = 0;
      while (XIter.hasNext()) {
        Tuple t = XIter.next();
        assertEquals(HCatFieldSchema.Type.numPrimitiveTypes(), t.size());
        int colPos = 0;
        for (Object referenceData : primitiveRows[numTuplesRead]) {
          if (referenceData == null) {
            assertTrue("rowNum=" + numTuplesRead + " colNum=" + colPos
                + " Reference data is null; actual "
                + t.get(colPos), t.get(colPos) == null);
          } else if (referenceData instanceof java.util.Date) {
            // Note that here we ignore nanos part of Hive Timestamp since nanos are dropped when
            // reading Hive from Pig by design.
            assertTrue("rowNum=" + numTuplesRead + " colNum=" + colPos
                + " Reference data=" + ((java.util.Date)referenceData).getTime()
                + " actual=" + ((DateTime)t.get(colPos)).getMillis()
                + "; types=(" + referenceData.getClass() + "," + t.get(colPos).getClass() + ")",
                ((java.util.Date)referenceData).getTime()== ((DateTime)t.get(colPos)).getMillis());
          } else {
            // Doing String comps here as value objects in Hive in Pig are different so equals()
            // doesn't work.
            assertTrue("rowNum=" + numTuplesRead + " colNum=" + colPos
                + " Reference data=" + referenceData + " actual=" + t.get(colPos)
                + "; types=(" + referenceData.getClass() + "," + t.get(colPos).getClass() + ") ",
                referenceData.toString().equals(t.get(colPos).toString()));
          }
          colPos++;
        }
        numTuplesRead++;
      }
      assertTrue("Expected " + primitiveRows.length + "; found " + numTuplesRead, numTuplesRead == primitiveRows.length);
    }
    private static void setupAllTypesTable(Driver driver) throws Exception {
      String[] primitiveData = new String[primitiveRows.length];
      for (int i = 0; i < primitiveRows.length; i++) {
        Object[] rowData = primitiveRows[i];
        StringBuilder row = new StringBuilder();
        for (Object cell : rowData) {
          row.append(row.length() == 0 ? "" : "\t").append(cell == null ? null : cell);
        }
        primitiveData[i] = row.toString();
      }
      HcatTestUtils.createTestDataFile(ALL_TYPES_FILE_NAME, primitiveData);
      String cmd = "create table " + ALL_PRIMITIVE_TYPES_TABLE + ALL_TYPES_SCHEMA +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
          " STORED AS TEXTFILE";
      executeStatementOnDriver(cmd, driver);
      cmd = "load data local inpath '" + HCatUtil.makePathASafeFileName(ALL_TYPES_FILE_NAME) +
          "' into table " + ALL_PRIMITIVE_TYPES_TABLE;
      executeStatementOnDriver(cmd, driver);
    }
  }
}
