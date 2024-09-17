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
import org.apache.hadoop.hive.conf.HiveConfForTest;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link GenericUDFFromUnixTime#evaluate(DeferredObject[])} with long value pattern inputs.
 */
@RunWith(Parameterized.class)
public class TestGenericUDFFromUnixTimeEvaluate {
  private final GenericUDFFromUnixTime udf = new GenericUDFFromUnixTime();
  private final ObjectInspector[] argInspectors =
      new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector };
  private final String expectedResult;
  private final String pattern;
  private final String zone;
  private final String formatter;
  private final long value;

  public TestGenericUDFFromUnixTimeEvaluate(String expectedResult, String pattern, String zone, String formatter,
      String value) {
    this.value = Long.parseLong(value);
    this.pattern = pattern;
    this.zone = zone;
    this.formatter = formatter;
    this.expectedResult = expectedResult;
  }

  @Parameterized.Parameters(name = "('{0}','{1}'), zone={2}, formatter={3}")
  public static Collection<String[]> readInputs() throws IOException, CsvException {
    CSVParser parser = new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(true).build();
    try (InputStream in = TestGenericUDFFromUnixTimeEvaluate.class.getResourceAsStream(
        "TestGenericUDFFromUnixTimeEvaluate.csv")) {
      Objects.requireNonNull(in);
      try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(in)).withCSVParser(parser).build()) {
        return reader.readAll();
      }
    }
  }

  @Test
  public void testEvaluate() throws HiveException, InterruptedException {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setVar(HiveConf.ConfVars.HIVE_DATETIME_FORMATTER, formatter);
    conf.setVar(HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE, zone);
    SessionState state = SessionState.start(conf);
    udf.initialize(argInspectors);
    try {
      Text result = (Text) udf.evaluate(new DeferredObject[] { new DeferredJavaObject(new LongWritable(value)), new DeferredJavaObject(new Text(pattern)) });
      assertEquals(udfDisplayWithInputs(), expectedResult, result.toString());
    }catch (RuntimeException e) {
      assertEquals(udfDisplayWithInputs(), expectedResult, e.getMessage());
    }
    SessionState.endStart(state);
  }

  private String udfDisplayWithInputs() {
    return udf.getDisplayString(new String[] { Long.toString(value), pattern }) + " sessionZone=" + zone + ", formatter="
        + formatter;
  }
}
