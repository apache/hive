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
package org.apache.hcatalog.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.HcatTestUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.Pair;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.TestHCatLoader} instead
 */
public class TestHCatLoader {
  private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(
          System.getProperty("java.io.tmpdir") + File.separator + TestHCatLoader.class.getCanonicalName() + "-" +
                  System.currentTimeMillis());
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String BASIC_FILE_NAME = TEST_DATA_DIR + "/basic.input.data";
  private static final String COMPLEX_FILE_NAME = TEST_DATA_DIR + "/complex.input.data";

  private static final String BASIC_TABLE = "junit_unparted_basic";
  private static final String COMPLEX_TABLE = "junit_unparted_complex";
  private static final String PARTITIONED_TABLE = "junit_parted_basic";
  private static final String SPECIFIC_SIZE_TABLE = "junit_specific_size";

  private Driver driver;
  private Map<Integer, Pair<Integer, String>> basicInputData;

  protected String storageFormat() {
    return "RCFILE tblproperties('hcat.isd'='org.apache.hcatalog.rcfile.RCFileInputDriver'," +
      "'hcat.osd'='org.apache.hcatalog.rcfile.RCFileOutputDriver')";
  }

  private void dropTable(String tablename) throws IOException, CommandNeedRetryException {
    driver.run("drop table " + tablename);
  }

  private void createTable(String tablename, String schema, String partitionedBy) throws IOException, CommandNeedRetryException {
    String createTable;
    createTable = "create table " + tablename + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      createTable = createTable + "partitioned by (" + partitionedBy + ") ";
    }
    createTable = createTable + "stored as " +storageFormat();
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table. [" + createTable + "], return code from hive driver : [" + retCode + "]");
    }
  }

  private void createTable(String tablename, String schema) throws IOException, CommandNeedRetryException {
    createTable(tablename, schema, null);
  }

  @Before
  public void setup() throws Exception {

    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if(!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }

    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));

    createTable(BASIC_TABLE, "a int, b string");
    createTable(COMPLEX_TABLE,
      "name string, studentid int, "
        + "contact struct<phno:string,email:string>, "
        + "currently_registered_courses array<string>, "
        + "current_grades map<string,string>, "
        + "phnos array<struct<phno:string,type:string>>");

    createTable(PARTITIONED_TABLE, "a int, b string", "bkt string");
    createTable(SPECIFIC_SIZE_TABLE, "a int, b string");

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
        //"Henry Jekyll\t42\t(415-253-6367,hjekyll@contemporary.edu.uk)\t{(PHARMACOLOGY),(PSYCHIATRY)},[PHARMACOLOGY#A-,PSYCHIATRY#B+],{(415-253-6367,cell),(408-253-6367,landline)}",
        //"Edward Hyde\t1337\t(415-253-6367,anonymous@b44chan.org)\t{(CREATIVE_WRITING),(COPYRIGHT_LAW)},[CREATIVE_WRITING#A+,COPYRIGHT_LAW#D],{(415-253-6367,cell),(408-253-6367,landline)}",
      }
    );

    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + BASIC_FILE_NAME + "' as (a:int, b:chararray);");

    server.registerQuery("store A into '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatStorer();");
    server.registerQuery("store A into '" + SPECIFIC_SIZE_TABLE + "' using org.apache.hcatalog.pig.HCatStorer();");
    server.registerQuery("B = foreach A generate a,b;");
    server.registerQuery("B2 = filter B by a < 2;");
    server.registerQuery("store B2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=0');");

    server.registerQuery("C = foreach A generate a,b;");
    server.registerQuery("C2 = filter C by a >= 2;");
    server.registerQuery("store C2 into '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatStorer('bkt=1');");

    server.registerQuery("D = load '" + COMPLEX_FILE_NAME + "' as (name:chararray, studentid:int, contact:tuple(phno:chararray,email:chararray), currently_registered_courses:bag{innertup:tuple(course:chararray)}, current_grades:map[ ] , phnos :bag{innertup:tuple(phno:chararray,type:chararray)});");
    server.registerQuery("store D into '" + COMPLEX_TABLE + "' using org.apache.hcatalog.pig.HCatStorer();");
    server.executeBatch();

  }

  @After
  public void tearDown() throws Exception {
    try {
      dropTable(BASIC_TABLE);
      dropTable(COMPLEX_TABLE);
      dropTable(PARTITIONED_TABLE);
      dropTable(SPECIFIC_SIZE_TABLE);
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }

  public void testSchemaLoadBasic() throws IOException {

    PigServer server = new PigServer(ExecType.LOCAL);

    // test that schema was loaded correctly
    server.registerQuery("X = load '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
    Schema dumpedXSchema = server.dumpSchema("X");
    List<FieldSchema> Xfields = dumpedXSchema.getFields();
    assertEquals(2, Xfields.size());
    assertTrue(Xfields.get(0).alias.equalsIgnoreCase("a"));
    assertTrue(Xfields.get(0).type == DataType.INTEGER);
    assertTrue(Xfields.get(1).alias.equalsIgnoreCase("b"));
    assertTrue(Xfields.get(1).type == DataType.CHARARRAY);

  }

  @Test
  public void testReadDataBasic() throws IOException {
    PigServer server = new PigServer(ExecType.LOCAL);

    server.registerQuery("X = load '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
    Iterator<Tuple> XIter = server.openIterator("X");
    int numTuplesRead = 0;
    while (XIter.hasNext()) {
      Tuple t = XIter.next();
      assertEquals(2, t.size());
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

    PigServer server = new PigServer(ExecType.LOCAL);

    // test that schema was loaded correctly
    server.registerQuery("K = load '" + COMPLEX_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
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
    PigServer server = new PigServer(ExecType.LOCAL);

    driver.run("select * from " + PARTITIONED_TABLE);
    ArrayList<String> valuesReadFromHiveDriver = new ArrayList<String>();
    driver.getResults(valuesReadFromHiveDriver);
    assertEquals(basicInputData.size(), valuesReadFromHiveDriver.size());

    server.registerQuery("W = load '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
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

    server.registerQuery("P1 = load '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
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

    server.registerQuery("P2 = load '" + PARTITIONED_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
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
  public void testProjectionsBasic() throws IOException {

    PigServer server = new PigServer(ExecType.LOCAL);

    // projections are handled by using generate, not "as" on the Load

    server.registerQuery("Y1 = load '" + BASIC_TABLE + "' using org.apache.hcatalog.pig.HCatLoader();");
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
      assertTrue(t.get(0).getClass() == Integer.class);
      assertEquals(t.get(0), basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    numTuplesRead = 0;
    Iterator<Tuple> Y3Iter = server.openIterator("Y3");
    while (Y3Iter.hasNext()) {
      Tuple t = Y3Iter.next();
      assertEquals(t.size(), 2);
      assertTrue(t.get(0).getClass() == String.class);
      assertEquals(t.get(0), basicInputData.get(numTuplesRead).second);
      assertTrue(t.get(1).getClass() == Integer.class);
      assertEquals(t.get(1), basicInputData.get(numTuplesRead).first);
      numTuplesRead++;
    }
    assertEquals(basicInputData.size(), numTuplesRead);
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

    String[] lines = new String[]{"llama\t1", "alpaca\t0"};
    HcatTestUtils.createTestDataFile(inputFileName, lines);

    assertEquals(0, driver.run("drop table if exists " + tbl).getResponseCode());
    assertEquals(0, driver.run("create external table " + tbl +
      " (a string, b boolean) row format delimited fields terminated by '\t'" +
      " stored as textfile location 'file:///" +
      inputDataDir.getPath().replaceAll("\\\\", "/") + "'").getResponseCode());

    Properties properties = new Properties();
    properties.setProperty(HCatConstants.HCAT_DATA_CONVERT_BOOLEAN_TO_INTEGER, "true");
    PigServer server = new PigServer(ExecType.LOCAL, properties);
    server.registerQuery(
      "data = load 'test_convert_boolean_to_int' using org.apache.hcatalog.pig.HCatLoader();");
    Schema schema = server.dumpSchema("data");
    assertEquals(2, schema.getFields().size());

    assertEquals("a", schema.getField(0).alias);
    assertEquals(DataType.CHARARRAY, schema.getField(0).type);
    assertEquals("b", schema.getField(1).alias);
    assertEquals(DataType.INTEGER, schema.getField(1).type);

    Iterator<Tuple> iterator = server.openIterator("data");
    Tuple t = iterator.next();
    assertEquals("llama", t.get(0));
    // TODO: Figure out how to load a text file into Hive with boolean columns. This next assert
    // passes because data was loaded as integers, not because it was converted.
    assertEquals(1, t.get(1));
    t = iterator.next();
    assertEquals("alpaca", t.get(0));
    assertEquals(0, t.get(1));
    assertFalse(iterator.hasNext());
  }
}
