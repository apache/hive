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

import com.google.common.collect.ImmutableSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormats;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.mapreduce.HCatBaseTest;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.LogUtils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class TestHCatStorer extends HCatBaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatStorer.class);

  private static final String INPUT_FILE_NAME = TEST_DATA_DIR + "/input.data";

  private static final Map<String, Set<String>> DISABLED_STORAGE_FORMATS =
    new HashMap<String, Set<String>>() {{
      put(IOConstants.AVRO, new HashSet<String>() {{
        add("testDateCharTypes"); // incorrect precision
          // expected:<0      xxxxx   yyy     5.2[]> but was:<0       xxxxx   yyy     5.2[0]>
        add("testWriteDecimalXY"); // incorrect precision
          // expected:<1.2[]> but was:<1.2[0]>
        add("testWriteSmallint");  // doesn't have a notion of small, and saves the full value as an int, so no overflow
          // expected:<null> but was:<32768>
        add("testWriteTimestamp"); // does not support timestamp
          // TypeInfoToSchema.createAvroPrimitive : UnsupportedOperationException
        add("testWriteTinyint"); // doesn't have a notion of tiny, and saves the full value as an int, so no overflow
          // expected:<null> but was:<300>
      }});
      put(IOConstants.PARQUETFILE, new HashSet<String>() {{
        add("testBagNStruct");
        add("testDateCharTypes");
        add("testDynamicPartitioningMultiPartColsInDataNoSpec");
        add("testDynamicPartitioningMultiPartColsInDataPartialSpec");
        add("testMultiPartColsInData");
        add("testPartColsInData");
        add("testStoreFuncAllSimpleTypes");
        add("testStoreFuncSimple");
        add("testStoreInPartiitonedTbl");
        add("testStoreMultiTables");
        add("testStoreWithNoCtorArgs");
        add("testStoreWithNoSchema");
        add("testWriteChar");
        add("testWriteDate");
        add("testWriteDate2");
        add("testWriteDate3");
        add("testWriteDecimal");
        add("testWriteDecimalX");
        add("testWriteDecimalXY");
        add("testWriteSmallint");
        add("testWriteTimestamp");
        add("testWriteTinyint");
        add("testWriteVarchar");
      }});
    }};

  private String storageFormat;

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return StorageFormats.names();
  }

  public TestHCatStorer(String storageFormat) {
    this.storageFormat = storageFormat;
  }

  //Start: tests that check values from Pig that are out of range for target column
  @Test
  public void testWriteTinyint() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    pigValueRangeTest("junitTypeTest1", "tinyint", "int", null, Integer.toString(1), Integer.toString(1));
    pigValueRangeTestOverflow("junitTypeTest1", "tinyint", "int", null, Integer.toString(300));
    pigValueRangeTestOverflow("junitTypeTest2", "tinyint", "int", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      Integer.toString(300));
    pigValueRangeTestOverflow("junitTypeTest3", "tinyint", "int", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      Integer.toString(300));
  }

  @Test
  public void testWriteSmallint() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    pigValueRangeTest("junitTypeTest1", "smallint", "int", null, Integer.toString(Short.MIN_VALUE),
      Integer.toString(Short.MIN_VALUE));
    pigValueRangeTestOverflow("junitTypeTest2", "smallint", "int", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      Integer.toString(Short.MAX_VALUE + 1));
    pigValueRangeTestOverflow("junitTypeTest3", "smallint", "int", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      Integer.toString(Short.MAX_VALUE + 1));
  }

  @Test
  public void testWriteChar() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    pigValueRangeTest("junitTypeTest1", "char(5)", "chararray", null, "xxx", "xxx  ");
    pigValueRangeTestOverflow("junitTypeTest1", "char(5)", "chararray", null, "too_long");
    pigValueRangeTestOverflow("junitTypeTest2", "char(5)", "chararray", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      "too_long");
    pigValueRangeTestOverflow("junitTypeTest3", "char(5)", "chararray", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      "too_long2");
  }

  @Test
  public void testWriteVarchar() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    pigValueRangeTest("junitTypeTest1", "varchar(5)", "chararray", null, "xxx", "xxx");
    pigValueRangeTestOverflow("junitTypeTest1", "varchar(5)", "chararray", null, "too_long");
    pigValueRangeTestOverflow("junitTypeTest2", "varchar(5)", "chararray", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      "too_long");
    pigValueRangeTestOverflow("junitTypeTest3", "varchar(5)", "chararray", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      "too_long2");
  }

  @Test
  public void testWriteDecimalXY() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    pigValueRangeTest("junitTypeTest1", "decimal(5,2)", "bigdecimal", null, BigDecimal.valueOf(1.2).toString(),
      BigDecimal.valueOf(1.2).toString());
    pigValueRangeTestOverflow("junitTypeTest1", "decimal(5,2)", "bigdecimal", null, BigDecimal.valueOf(12345.12).toString());
    pigValueRangeTestOverflow("junitTypeTest2", "decimal(5,2)", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      BigDecimal.valueOf(500.123).toString());
    pigValueRangeTestOverflow("junitTypeTest3", "decimal(5,2)", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      BigDecimal.valueOf(500.123).toString());
  }

  @Test
  public void testWriteDecimalX() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    //interestingly decimal(2) means decimal(2,0)
    pigValueRangeTest("junitTypeTest1", "decimal(2)", "bigdecimal", null, BigDecimal.valueOf(12).toString(),
      BigDecimal.valueOf(12).toString());
    pigValueRangeTestOverflow("junitTypeTest2", "decimal(2)", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      BigDecimal.valueOf(50.123).toString());
    pigValueRangeTestOverflow("junitTypeTest3", "decimal(2)", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      BigDecimal.valueOf(50.123).toString());
  }

  @Test
  public void testWriteDecimal() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    //decimal means decimal(10,0)
    pigValueRangeTest("junitTypeTest1", "decimal", "bigdecimal", null, BigDecimal.valueOf(1234567890).toString(),
      BigDecimal.valueOf(1234567890).toString());
    pigValueRangeTestOverflow("junitTypeTest2", "decimal", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      BigDecimal.valueOf(12345678900L).toString());
    pigValueRangeTestOverflow("junitTypeTest3", "decimal", "bigdecimal", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      BigDecimal.valueOf(12345678900L).toString());
  }

  /**
   * because we want to ignore TZ which is included in toString()
   * include time to make sure it's 0
   */
  private static final String FORMAT_4_DATE = "yyyy-MM-dd HH:mm:ss";

  @Test
  public void testWriteDate() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    DateTime d = new DateTime(1991,10,11,0,0);
    pigValueRangeTest("junitTypeTest1", "date", "datetime", null, d.toString(),
      d.toString(FORMAT_4_DATE), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest2", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.plusHours(2).toString(), FORMAT_4_DATE);//time != 0
    pigValueRangeTestOverflow("junitTypeTest3", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.plusMinutes(1).toString(), FORMAT_4_DATE);//time != 0
    d = new DateTime(1991,10,11,0,0,DateTimeZone.forOffsetHours(-11));
    pigValueRangeTest("junitTypeTest4", "date", "datetime", null, d.toString(),
      d.toString(FORMAT_4_DATE), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest5", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.plusHours(2).toString(), FORMAT_4_DATE);//date out of range due to time != 0
    pigValueRangeTestOverflow("junitTypeTest6", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.plusMinutes(1).toString(), FORMAT_4_DATE);//date out of range due to time!=0
  }

  @Test
  public void testWriteDate3() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    DateTime d = new DateTime(1991,10,11,23,10,DateTimeZone.forOffsetHours(-11));
    FrontendException fe = null;
    //expect to fail since the time component is not 0
    pigValueRangeTestOverflow("junitTypeTest4", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.toString(), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest5", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.plusHours(2).toString(), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest6", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.plusMinutes(1).toString(), FORMAT_4_DATE);
  }

  @Test
  public void testWriteDate2() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    DateTime d = new DateTime(1991,11,12,0,0, DateTimeZone.forID("US/Eastern"));
    pigValueRangeTest("junitTypeTest1", "date", "datetime", null, d.toString(),
      d.toString(FORMAT_4_DATE), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest2", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.plusHours(2).toString(), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest2", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.plusMillis(20).toString(), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest2", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.plusMillis(12).toString(), FORMAT_4_DATE);
    pigValueRangeTestOverflow("junitTypeTest3", "date", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Throw,
      d.plusMinutes(1).toString(), FORMAT_4_DATE);
  }

  /**
   * Note that the value that comes back from Hive will have local TZ on it.  Using local is
   * arbitrary but DateTime needs TZ (or will assume default) and Hive does not have TZ.
   * So if you start with Pig value in TZ=x and write to Hive, when you read it back the TZ may
   * be different.  The millis value should match, of course.
   *
   * @throws Exception
   */
  @Test
  public void testWriteTimestamp() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    DateTime d = new DateTime(1991,10,11,14,23,30, 10);//uses default TZ
    pigValueRangeTest("junitTypeTest1", "timestamp", "datetime", null, d.toString(),
      d.toDateTime(DateTimeZone.getDefault()).toString());
    d = d.plusHours(2);
    pigValueRangeTest("junitTypeTest2", "timestamp", "datetime", HCatBaseStorer.OOR_VALUE_OPT_VALUES.Null,
      d.toString(), d.toDateTime(DateTimeZone.getDefault()).toString());
    d = d.toDateTime(DateTimeZone.UTC);
    pigValueRangeTest("junitTypeTest3", "timestamp", "datetime", null, d.toString(),
      d.toDateTime(DateTimeZone.getDefault()).toString());

    d = new DateTime(1991,10,11,23,24,25, 26);
    pigValueRangeTest("junitTypeTest1", "timestamp", "datetime", null, d.toString(),
      d.toDateTime(DateTimeZone.getDefault()).toString());
    d = d.toDateTime(DateTimeZone.UTC);
    pigValueRangeTest("junitTypeTest3", "timestamp", "datetime", null, d.toString(),
      d.toDateTime(DateTimeZone.getDefault()).toString());
  }
  //End: tests that check values from Pig that are out of range for target column

  private void pigValueRangeTestOverflow(String tblName, String hiveType, String pigType,
    HCatBaseStorer.OOR_VALUE_OPT_VALUES goal, String inputValue, String format) throws Exception {
    pigValueRangeTest(tblName, hiveType, pigType, goal, inputValue, null, format);
  }

  private void pigValueRangeTestOverflow(String tblName, String hiveType, String pigType,
                                 HCatBaseStorer.OOR_VALUE_OPT_VALUES goal, String inputValue) throws Exception {
    pigValueRangeTest(tblName, hiveType, pigType, goal, inputValue, null, null);
  }

  private void pigValueRangeTest(String tblName, String hiveType, String pigType,
                                 HCatBaseStorer.OOR_VALUE_OPT_VALUES goal, String inputValue,
                                 String expectedValue) throws Exception {
    pigValueRangeTest(tblName, hiveType, pigType, goal, inputValue, expectedValue, null);
  }

  /**
   * This is used to test how Pig values of various data types which are out of range for Hive target
   * column are handled.  Currently the options are to raise an error or write NULL.
   * 1. create a data file with 1 column, 1 row
   * 2. load into pig
   * 3. use pig to store into Hive table
   * 4. read from Hive table using Pig
   * 5. check that read value is what is expected
   * @param tblName Hive table name to create
   * @param hiveType datatype to use for the single column in table
   * @param pigType corresponding Pig type when loading file into Pig
   * @param goal how out-of-range values from Pig are handled by HCat, may be {@code null}
   * @param inputValue written to file which is read by Pig, thus must be something Pig can read
   *                   (e.g. DateTime.toString(), rather than java.sql.Date)
   * @param expectedValue what Pig should see when reading Hive table
   * @param format date format to use for comparison of values since default DateTime.toString()
   *               includes TZ which is meaningless for Hive DATE type
   */
  private void pigValueRangeTest(String tblName, String hiveType, String pigType,
                                 HCatBaseStorer.OOR_VALUE_OPT_VALUES goal, String inputValue, String expectedValue, String format)
    throws Exception {
    TestHCatLoader.dropTable(tblName, driver);
    final String field = "f1";
    TestHCatLoader.createTable(tblName, field + " " + hiveType, null, driver, storageFormat);
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, new String[] {inputValue});
    LOG.debug("File=" + INPUT_FILE_NAME);
    dumpFile(INPUT_FILE_NAME);
    PigServer server = createPigServer(true);
    int queryNumber = 1;
    logAndRegister(server,
      "A = load '" + INPUT_FILE_NAME + "' as (" + field + ":" + pigType + ");", queryNumber++);
    Iterator<Tuple> firstLoad = server.openIterator("A");
    if(goal == null) {
      logAndRegister(server,
        "store A into '" + tblName + "' using " + HCatStorer.class.getName() + "();", queryNumber++);
    }
    else {
      FrontendException fe = null;
      try {
        logAndRegister(server,
          "store A into '" + tblName + "' using " + HCatStorer.class.getName() + "('','','-" +
          HCatStorer.ON_OOR_VALUE_OPT + " " + goal + "');",
          queryNumber++);
      }
      catch(FrontendException e) {
        fe = e;
      }
      switch (goal) {
        case Null:
          //do nothing, fall through and verify the data
          break;
        case Throw:
          assertTrue("Expected a FrontendException", fe != null);
          assertEquals("Expected a different FrontendException.", fe.getMessage(), "Unable to store alias A");
          return;//this test is done
        default:
          assertFalse("Unexpected goal: " + goal, 1 == 1);
      }
    }
    logAndRegister(server, "B = load '" + tblName + "' using " + HCatLoader.class.getName() + "();", queryNumber);
    CommandProcessorResponse cpr = driver.run("select * from " + tblName);
    LOG.debug("cpr.respCode=" + cpr.getResponseCode() + " cpr.errMsg=" + cpr.getErrorMessage() +
      " for table " + tblName);
    List l = new ArrayList();
    driver.getResults(l);
    LOG.debug("Dumping rows via SQL from " + tblName);
    for(Object t : l) {
      LOG.debug(t == null ? null : t.toString() + " t.class=" + t.getClass());
    }
    Iterator<Tuple> itr = server.openIterator("B");
    int numRowsRead = 0;
    while(itr.hasNext()) {
      Tuple t = itr.next();
      if("date".equals(hiveType)) {
        DateTime dateTime = (DateTime)t.get(0);
        assertTrue(format != null);
        assertEquals("Comparing Pig to Raw data for table " + tblName, expectedValue, dateTime== null ? null : dateTime.toString(format));
      }
      else {
        assertEquals("Comparing Pig to Raw data for table " + tblName, expectedValue, t.isNull(0) ? null : t.get(0).toString());
      }
      //see comment at "Dumping rows via SQL..." for why this doesn't work
      //assertEquals("Comparing Pig to Hive", t.get(0), l.get(0));
      numRowsRead++;
    }
    assertEquals("Expected " + 1 + " rows; got " + numRowsRead + " file=" + INPUT_FILE_NAME + "; table " +
      tblName, 1, numRowsRead);
    /* Misc notes:
    Unfortunately Timestamp.toString() adjusts the value for local TZ and 't' is a String
    thus the timestamp in 't' doesn't match rawData*/
  }

  /**
   * Create a data file with datatypes added in 0.13.  Read it with Pig and use
   * Pig + HCatStorer to write to a Hive table.  Then read it using Pig and Hive
   * and make sure results match.
   */
  @Test
  public void testDateCharTypes() throws Exception {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    final String tblName = "junit_date_char";
    TestHCatLoader.dropTable(tblName, driver);
    TestHCatLoader.createTable(tblName,
      "id int, char5 char(5), varchar10 varchar(10), dec52 decimal(5,2)", null, driver, storageFormat);
    int NUM_ROWS = 5;
    String[] rows = new String[NUM_ROWS];
    for(int i = 0; i < NUM_ROWS; i++) {
      //since the file is read by Pig, we need to make sure the values are in format that Pig understands
      //otherwise it will turn the value to NULL on read
      rows[i] = i + "\txxxxx\tyyy\t" + 5.2;
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, rows);
    LOG.debug("File=" + INPUT_FILE_NAME);
//    dumpFile(INPUT_FILE_NAME);
    PigServer server = createPigServer(true);
    int queryNumber = 1;
    logAndRegister(server,
      "A = load '" + INPUT_FILE_NAME + "' as (id:int, char5:chararray, varchar10:chararray, dec52:bigdecimal);",
      queryNumber++);
    logAndRegister(server,
      "store A into '" + tblName + "' using " + HCatStorer.class.getName() + "();", queryNumber++);
    logAndRegister(server, "B = load '" + tblName + "' using " + HCatLoader.class.getName() + "();",
      queryNumber);
    CommandProcessorResponse cpr = driver.run("select * from " + tblName);
    LOG.debug("cpr.respCode=" + cpr.getResponseCode() + " cpr.errMsg=" + cpr.getErrorMessage());
    List l = new ArrayList();
    driver.getResults(l);
    LOG.debug("Dumping rows via SQL from " + tblName);
      /*Unfortunately Timestamp.toString() adjusts the value for local TZ and 't' is a String
      * thus the timestamp in 't' doesn't match rawData*/
    for(Object t : l) {
      LOG.debug(t == null ? null : t.toString());
    }
    Iterator<Tuple> itr = server.openIterator("B");
    int numRowsRead = 0;
    while (itr.hasNext()) {
      Tuple t = itr.next();
      StringBuilder rowFromPig = new StringBuilder();
      for(int i = 0; i < t.size(); i++) {
        rowFromPig.append(t.get(i)).append("\t");
      }
      rowFromPig.setLength(rowFromPig.length() - 1);
      assertEquals("Comparing Pig to Raw data", rows[numRowsRead], rowFromPig.toString());
      //see comment at "Dumping rows via SQL..." for why this doesn't work (for all types)
      //assertEquals("Comparing Pig to Hive", rowFromPig.toString(), l.get(numRowsRead));
      numRowsRead++;
    }
    assertEquals("Expected " + NUM_ROWS + " rows; got " + numRowsRead + " file=" + INPUT_FILE_NAME, NUM_ROWS, numRowsRead);
  }

  static void dumpFile(String fileName) throws Exception {
    File file = new File(fileName);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = null;
    LOG.debug("Dumping raw file: " + fileName);
    while((line = reader.readLine()) != null) {
      LOG.debug(line);
    }
    reader.close();
  }

  @Test
  public void testPartColsInData() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int) partitioned by (b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];
    for (int i = 0; i < LOOP_SIZE; i++) {
      input[i] = i + "\t1";
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('b=1');");
    server.registerQuery("B = load 'default.junit_unparted' using " + HCatLoader.class.getName() + "();");
    Iterator<Tuple> itr = server.openIterator("B");

    int i = 0;

    while (itr.hasNext()) {
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertEquals(t.get(0), i);
      assertEquals(t.get(1), "1");
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(LOOP_SIZE, i);
  }

  @Test
  public void testMultiPartColsInData() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
      " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS " + storageFormat;

    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
      "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
      "111239\tSatya\t01/01/2001\tM\tIN\tKL",
      "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '" + INPUT_FILE_NAME + "' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
      "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("TN = FILTER A BY emp_state == 'TN';");
    pig.registerQuery("KA = FILTER A BY emp_state == 'KA';");
    pig.registerQuery("KL = FILTER A BY emp_state == 'KL';");
    pig.registerQuery("AP = FILTER A BY emp_state == 'AP';");
    pig.registerQuery("STORE TN INTO 'employee' USING " + HCatStorer.class.getName() + "('emp_country=IN,emp_state=TN');");
    pig.registerQuery("STORE KA INTO 'employee' USING " + HCatStorer.class.getName() + "('emp_country=IN,emp_state=KA');");
    pig.registerQuery("STORE KL INTO 'employee' USING " + HCatStorer.class.getName() + "('emp_country=IN,emp_state=KL');");
    pig.registerQuery("STORE AP INTO 'employee' USING " + HCatStorer.class.getName() + "('emp_country=IN,emp_state=AP');");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

  @Test
  public void testStoreInPartiitonedTbl() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int) partitioned by (b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];
    for (int i = 0; i < LOOP_SIZE; i++) {
      input[i] = i + "";
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int);");
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('b=1');");
    server.registerQuery("B = load 'default.junit_unparted' using " + HCatLoader.class.getName() + "();");
    Iterator<Tuple> itr = server.openIterator("B");

    int i = 0;

    while (itr.hasNext()) {
      Tuple t = itr.next();
      assertEquals(2, t.size());
      assertEquals(t.get(0), i);
      assertEquals(t.get(1), "1");
      i++;
    }

    assertFalse(itr.hasNext());
    assertEquals(11, i);
  }

  @Test
  public void testNoAlias() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    driver.run("drop table junit_parted");
    String createTable = "create table junit_parted(a int, b string) partitioned by (ds string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    PigServer server = new PigServer(ExecType.LOCAL);
    boolean errCaught = false;
    try {
      server.setBatchOn();
      server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
      server.registerQuery("B = foreach A generate a+10, b;");
      server.registerQuery("store B into 'junit_parted' using " + HCatStorer.class.getName() + "('ds=20100101');");
      server.executeBatch();
    } catch (PigException fe) {
      PigException pe = LogUtils.getPigException(fe);
      assertTrue(pe instanceof FrontendException);
      assertEquals(PigHCatUtil.PIG_EXCEPTION_CODE, pe.getErrorCode());
      assertTrue(pe.getMessage().contains("Column name for a field is not specified. Please provide the full schema as an argument to HCatStorer."));
      errCaught = true;
    }
    assertTrue(errCaught);
    errCaught = false;
    try {
      server.setBatchOn();
      server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, B:chararray);");
      server.registerQuery("B = foreach A generate a, B;");
      server.registerQuery("store B into 'junit_parted' using " + HCatStorer.class.getName() + "('ds=20100101');");
      server.executeBatch();
    } catch (PigException fe) {
      PigException pe = LogUtils.getPigException(fe);
      assertTrue(pe instanceof FrontendException);
      assertEquals(PigHCatUtil.PIG_EXCEPTION_CODE, pe.getErrorCode());
      assertTrue(pe.getMessage().contains("Column names should all be in lowercase. Invalid name found: B"));
      errCaught = true;
    }
    driver.run("drop table junit_parted");
    assertTrue(errCaught);
  }

  @Test
  public void testStoreMultiTables() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    driver.run("drop table junit_unparted2");
    createTable = "create table junit_unparted2(a int, b string) stored as RCFILE";
    retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        input[k++] = si + "\t" + j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a < 2;");
    server.registerQuery("store B into 'junit_unparted' using " + HCatStorer.class.getName() + "();");
    server.registerQuery("C = filter A by a >= 2;");
    server.registerQuery("store C into 'junit_unparted2' using " + HCatStorer.class.getName() + "();");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("select * from junit_unparted2");
    ArrayList<String> res2 = new ArrayList<String>();
    driver.getResults(res2);

    res.addAll(res2);
    driver.run("drop table junit_unparted");
    driver.run("drop table junit_unparted2");

    Iterator<String> itr = res.iterator();
    for (int i = 0; i < LOOP_SIZE * LOOP_SIZE; i++) {
      assertEquals(input[i], itr.next());
    }

    assertFalse(itr.hasNext());

  }

  @Test
  public void testStoreWithNoSchema() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        input[k++] = si + "\t" + j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for (int i = 0; i < LOOP_SIZE * LOOP_SIZE; i++) {
      assertEquals(input[i], itr.next());
    }

    assertFalse(itr.hasNext());

  }

  @Test
  public void testStoreWithNoCtorArgs() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        input[k++] = si + "\t" + j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into 'junit_unparted' using " + HCatStorer.class.getName() + "();");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for (int i = 0; i < LOOP_SIZE * LOOP_SIZE; i++) {
      assertEquals(input[i], itr.next());
    }

    assertFalse(itr.hasNext());

  }

  @Test
  public void testEmptyStore() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] input = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        input[k++] = si + "\t" + j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("B = filter A by a > 100;");
    server.registerQuery("store B into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('','a:int,b:chararray');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    assertFalse(itr.hasNext());

  }

  @Test
  public void testBagNStruct() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));
    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(b string,a struct<a1:int>,  arr_of_struct array<string>, " +
      "arr_of_struct2 array<struct<s1:string,s2:string>>,  arr_of_struct3 array<struct<s3:string>>) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = new String[]{"zookeeper\t(2)\t{(pig)}\t{(pnuts,hdfs)}\t{(hadoop),(hcat)}",
      "chubby\t(2)\t{(sawzall)}\t{(bigtable,gfs)}\t{(mapreduce),(hcat)}"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);

    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (b:chararray, a:tuple(a1:int), arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)});");
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('','b:chararray, a:tuple(a1:int)," +
      " arr_of_struct:bag{mytup:tuple(s1:chararray)}, arr_of_struct2:bag{mytup:tuple(s1:chararray,s2:chararray)}, arr_of_struct3:bag{t3:tuple(s3:chararray)}');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    assertEquals("zookeeper\t{\"a1\":2}\t[\"pig\"]\t[{\"s1\":\"pnuts\",\"s2\":\"hdfs\"}]\t[{\"s3\":\"hadoop\"},{\"s3\":\"hcat\"}]", itr.next());
    assertEquals("chubby\t{\"a1\":2}\t[\"sawzall\"]\t[{\"s1\":\"bigtable\",\"s2\":\"gfs\"}]\t[{\"s3\":\"mapreduce\"},{\"s3\":\"hcat\"}]", itr.next());
    assertFalse(itr.hasNext());

  }

  @Test
  public void testStoreFuncAllSimpleTypes() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b float, c double, d bigint, e string, h boolean, f binary, g binary) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int i = 0;
    String[] input = new String[3];
    input[i++] = "0\t\t\t\t\t\t\t"; //Empty values except first column
    input[i++] = "\t" + i * 2.1f + "\t" + i * 1.1d + "\t" + i * 2L + "\t" + "lets hcat" + "\t" + "true" + "\tbinary-data"; //First column empty
    input[i++] = i + "\t" + i * 2.1f + "\t" + i * 1.1d + "\t" + i * 2L + "\t" + "lets hcat" + "\t" + "false" + "\tbinary-data";

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:float, c:double, d:long, e:chararray, h:boolean, f:bytearray);");
    //null gets stored into column g which is a binary field.
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('','a:int, b:float, c:double, d:long, e:chararray, h:boolean, f:bytearray');");
    server.executeBatch();


    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);

    Iterator<String> itr = res.iterator();
    String next = itr.next();
    assertEquals("0\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL", next );
    assertEquals("NULL\t4.2\t2.2\t4\tlets hcat\ttrue\tbinary-data\tNULL", itr.next());
    assertEquals("3\t6.2999997\t3.3000000000000003\t6\tlets hcat\tfalse\tbinary-data\tNULL", itr.next());
    assertFalse(itr.hasNext());

    server.registerQuery("B = load 'junit_unparted' using " + HCatLoader.class.getName() + ";");
    Iterator<Tuple> iter = server.openIterator("B");
    int count = 0;
    int num5nulls = 0;
    while (iter.hasNext()) {
      Tuple t = iter.next();
      if (t.get(6) == null) {
        num5nulls++;
      } else {
        assertTrue(t.get(6) instanceof DataByteArray);
      }
      assertNull(t.get(7));
      count++;
    }
    assertEquals(3, count);
    assertEquals(1, num5nulls);
    driver.run("drop table junit_unparted");
  }

  @Test
  public void testStoreFuncSimple() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table junit_unparted");
    String createTable = "create table junit_unparted(a int, b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    int LOOP_SIZE = 3;
    String[] inputData = new String[LOOP_SIZE * LOOP_SIZE];
    int k = 0;
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        inputData[k++] = si + "\t" + j;
      }
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME + "' as (a:int, b:chararray);");
    server.registerQuery("store A into 'default.junit_unparted' using " + HCatStorer.class.getName() + "('','a:int,b:chararray');");
    server.executeBatch();

    driver.run("select * from junit_unparted");
    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    driver.run("drop table junit_unparted");
    Iterator<String> itr = res.iterator();
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        assertEquals(si + "\t" + j, itr.next());
      }
    }
    assertFalse(itr.hasNext());

  }

  @Test
  public void testDynamicPartitioningMultiPartColsInDataPartialSpec() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table if exists employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
      " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS " + storageFormat;

    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
      "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
      "111239\tSatya\t01/01/2001\tM\tIN\tKL",
      "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '" + INPUT_FILE_NAME + "' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
      "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
    pig.registerQuery("STORE IN INTO 'employee' USING " + HCatStorer.class.getName() + "('emp_country=IN');");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

  @Test
  public void testDynamicPartitioningMultiPartColsInDataNoSpec() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table if exists employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
      " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS " + storageFormat;

    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {"111237\tKrishna\t01/01/1990\tM\tIN\tTN",
      "111238\tKalpana\t01/01/2000\tF\tIN\tKA",
      "111239\tSatya\t01/01/2001\tM\tIN\tKL",
      "111240\tKavya\t01/01/2002\tF\tIN\tAP"};

    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);
    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '" + INPUT_FILE_NAME + "' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
      "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
    pig.registerQuery("STORE IN INTO 'employee' USING " + HCatStorer.class.getName() + "();");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(4, results.size());
    Collections.sort(results);
    assertEquals(inputData[0], results.get(0));
    assertEquals(inputData[1], results.get(1));
    assertEquals(inputData[2], results.get(2));
    assertEquals(inputData[3], results.get(3));
    driver.run("drop table employee");
  }

  @Test
  public void testDynamicPartitioningMultiPartColsNoDataInDataNoSpec() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table if exists employee");
    String createTable = "CREATE TABLE employee (emp_id INT, emp_name STRING, emp_start_date STRING , emp_gender STRING ) " +
      " PARTITIONED BY (emp_country STRING , emp_state STRING ) STORED AS " + storageFormat;

    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }

    String[] inputData = {};
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, inputData);

    PigServer pig = new PigServer(ExecType.LOCAL);
    pig.setBatchOn();
    pig.registerQuery("A = LOAD '" + INPUT_FILE_NAME + "' USING PigStorage() AS (emp_id:int,emp_name:chararray,emp_start_date:chararray," +
      "emp_gender:chararray,emp_country:chararray,emp_state:chararray);");
    pig.registerQuery("IN = FILTER A BY emp_country == 'IN';");
    pig.registerQuery("STORE IN INTO 'employee' USING " + HCatStorer.class.getName() + "();");
    pig.executeBatch();
    driver.run("select * from employee");
    ArrayList<String> results = new ArrayList<String>();
    driver.getResults(results);
    assertEquals(0, results.size());
    driver.run("drop table employee");
  }

  @Test
  public void testPartitionPublish() throws IOException, CommandNeedRetryException {
    assumeTrue(!TestUtil.shouldSkip(storageFormat, DISABLED_STORAGE_FORMATS));

    driver.run("drop table ptn_fail");
    String createTable = "create table ptn_fail(a int, c string) partitioned by (b string) stored as " + storageFormat;
    int retCode = driver.run(createTable).getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table.");
    }
    int LOOP_SIZE = 11;
    String[] input = new String[LOOP_SIZE];

    for (int i = 0; i < LOOP_SIZE; i++) {
      input[i] = i + "\tmath";
    }
    HcatTestUtils.createTestDataFile(INPUT_FILE_NAME, input);
    PigServer server = new PigServer(ExecType.LOCAL);
    server.setBatchOn();
    server.registerQuery("A = load '" + INPUT_FILE_NAME
        + "' as (a:int, c:chararray);");
    server.registerQuery("B = filter A by " + FailEvalFunc.class.getName()
      + "($0);");
    server.registerQuery("store B into 'ptn_fail' using "
      + HCatStorer.class.getName() + "('b=math');");
    server.executeBatch();

    String query = "show partitions ptn_fail";
    retCode = driver.run(query).getResponseCode();

    if (retCode != 0) {
      throw new IOException("Error " + retCode + " running query "
          + query);
    }

    ArrayList<String> res = new ArrayList<String>();
    driver.getResults(res);
    assertEquals(0, res.size());

    // Make sure the partitions directory is not in hdfs.
    assertTrue((new File(TEST_WAREHOUSE_DIR + "/ptn_fail")).exists());
    assertFalse((new File(TEST_WAREHOUSE_DIR + "/ptn_fail/b=math"))
      .exists());
  }

  static public class FailEvalFunc extends EvalFunc<Boolean> {

    /*
     * @param Tuple /* @return null /* @throws IOException
     *
     * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
     */
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
      throw new IOException("Eval Func to mimic Failure.");
    }

  }
}
