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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
 * Tests for {@link GenericUDFDateFormat#evaluate(DeferredObject[])} with string value inputs.
 */
@RunWith(Parameterized.class)
public class TestGenericUDFDateFormatEvaluate {

  private final GenericUDFDateFormat udf = new GenericUDFDateFormat();
  private final String value;
  private final String pattern;
  private final String zone;
  private final String formatter;
  private final Text expectedResult;

  public TestGenericUDFDateFormatEvaluate(String value, String pattern, String zone, String formatter,
      String expectedResult) {
    this.value = value;
    this.pattern = pattern;
    this.zone = zone;
    this.formatter = formatter;
    this.expectedResult = expectedResult.equals("null") ? null : new Text(expectedResult);
  }

  @Parameterized.Parameters(name = "date_format('{0}','{1}'), zone={2}, formatter={3}")
  public static Collection<String[]> readInputs() throws IOException, CsvException {
    CSVParser parser = new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(true).build();
    try (InputStream in = TestGenericUDFDateFormatEvaluate.class.getResourceAsStream(
        "TestGenericUDFDateFormatEvaluate.csv")) {
      Objects.requireNonNull(in);
      try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(in))
          .withSkipLines(1) // Skip header
          .withCSVParser(parser)
          .build()) {
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
    ObjectInspector[] argInspectors =
        new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo,
                new Text(pattern)) };
    udf.initialize(argInspectors);
    Text result = (Text) udf.evaluate(new DeferredObject[] { new DeferredJavaObject(new Text(value)) });
    assertEquals(udfDisplayWithInputs(), expectedResult, result);
    SessionState.endStart(state);
  }

  private String udfDisplayWithInputs() {
    return udf.getDisplayString(new String[] { value, pattern }) + " sessionZone=" + zone + ", formatter=" + formatter;
  }
}
