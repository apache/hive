/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.util.Collection;
import java.util.Objects;

@RunWith(Parameterized.class)
public class TestVectorUDFUnixTimeStampString {

  private final String value;
  private final String zone;
  private final String formatter;
  private final Long expectedResult;

  public TestVectorUDFUnixTimeStampString(String value, String zone, String formatter, String expectedResult) {
    this.value = value;
    this.zone = zone;
    this.formatter = formatter;
    this.expectedResult = expectedResult.equals("null") ? null : Long.parseLong(expectedResult);
  }

  @Parameterized.Parameters(name = "('{0}'), zone={1}, parserLegacy={2}")
  public static Collection<String[]> readInputs() throws IOException, CsvException {
    CSVParser parser = new CSVParserBuilder().withSeparator(';').withIgnoreQuotations(true).build();
    try (InputStream in = TestVectorUDFUnixTimeStampString.class.getResourceAsStream(
        "TestVectorUnixTimeStampString.csv")) {
      Objects.requireNonNull(in);
      try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(in)).withCSVParser(parser).build()) {
        return reader.readAll();
      }
    }
  }

  @Test
  public void testEvaluate() throws HiveException, InterruptedException, CharacterCodingException {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setVar(HiveConf.ConfVars.HIVE_DATETIME_FORMATTER, formatter);
    conf.setVar(HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE, zone);
    SessionState state = SessionState.start(conf);
    VectorUDFUnixTimeStampString udf = new VectorUDFUnixTimeStampString(0, 1);
    udf.setInputTypeInfos(TypeInfoFactory.stringTypeInfo);
    udf.transientInit(conf);
    VectorizedRowBatch batch = singleElementRowBatch(value);
    udf.evaluate(batch);
    LongColumnVector result = (LongColumnVector) batch.cols[1];
    if (expectedResult == null) {
      Assert.assertTrue(udfDisplayWithInputs(), result.isNull[0]);
    } else {
      Assert.assertEquals(udfDisplayWithInputs(), expectedResult.longValue(), result.vector[0]);
    }
    SessionState.endStart(state);
  }

  private String udfDisplayWithInputs() {
    return "unix_timestamp(" + value + ") sessionZone=" + zone + ", legacy=" + formatter;
  }

  private static VectorizedRowBatch singleElementRowBatch(String e) throws CharacterCodingException {
    BytesColumnVector bcv = new BytesColumnVector();
    byte[] encoded = Text.encode(e).array();
    bcv.vector[0] = encoded;
    bcv.start[0] = 0;
    bcv.length[0] = encoded.length;

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    batch.cols[0] = bcv;
    batch.cols[1] = new LongColumnVector();
    batch.size = 1;
    return batch;
  }
}
