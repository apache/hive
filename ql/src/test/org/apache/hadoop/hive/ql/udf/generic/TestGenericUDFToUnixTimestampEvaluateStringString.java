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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GenericUDFToUnixTimeStamp#evaluate(DeferredObject[])} with string value and pattern inputs.
 */
@RunWith(Parameterized.class)
public class TestGenericUDFToUnixTimestampEvaluateStringString {
  private final GenericUDFToUnixTimeStamp udf = new GenericUDFToUnixTimeStamp();
  private final GenericUDFUnixTimeStamp udfUnixTimeStamp = new GenericUDFUnixTimeStamp();
  private final ObjectInspector[] argInspectors = new ObjectInspector[2];
  private final String value;
  private final String pattern;
  private final String zone;
  private final String formatter;
  private final String resolverStyle;
  private final LongWritable expectedResult;

  public TestGenericUDFToUnixTimestampEvaluateStringString(String value, String pattern, String zone, String formatter,
      String resolverStyle, String expectedResult) {
    this.value = value;
    this.pattern = pattern;
    this.zone = zone;
    this.formatter = formatter;
    this.resolverStyle = resolverStyle;
    this.expectedResult = expectedResult.equals("null") ? null : new LongWritable(Long.parseLong(expectedResult));
    Arrays.fill(argInspectors, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Parameterized.Parameters(name = "('{0}','{1}'), zone={2}, parserLegacy={3}, resolverStyle={4}")
  public static Collection<String[]> readInputs() throws IOException, CsvException {
    CSVParser parser = new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(true).build();
    try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(
        TestGenericUDFToUnixTimestampEvaluateStringString.class.getResourceAsStream(
            "TestGenericUDFToUnixTimestampEvaluateStringString.csv"))).withCSVParser(parser).build()) {
      return reader.readAll();
    }
  }

  @Test
  public void testEvaluateToUnixTimeStamp() throws HiveException, InterruptedException {
    testEvaluateWithUDF(udf);
  }

  @Test
  public void testEvaluateUnixTimeStamp() throws HiveException, InterruptedException {
    testEvaluateWithUDF(udfUnixTimeStamp);
  }

  private void testEvaluateWithUDF(GenericUDF udfToTest) throws HiveException, InterruptedException {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_DATETIME_FORMATTER, formatter);
    conf.setVar(HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE, zone);
    conf.setVar(HiveConf.ConfVars.HIVE_DATETIME_RESOLVER_STYLE, resolverStyle);
    SessionState state = SessionState.start(conf);
    udfToTest.initialize(argInspectors);
    LongWritable result = (LongWritable) udfToTest.evaluate(
        new DeferredObject[] { new DeferredJavaObject(new Text(value)), new DeferredJavaObject(new Text(pattern)) });
    assertEquals(udfDisplayWithInputs(udfToTest), expectedResult, result);
    SessionState.endStart(state);
  }

  private String udfDisplayWithInputs(GenericUDF udf) {
    return udf.getDisplayString(new String[] { value, pattern }) + " sessionZone=" + zone + ", formatter=" + formatter
        + ", resolver Style=" + resolverStyle;
  }
}
