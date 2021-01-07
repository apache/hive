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

package org.apache.hadoop.hive.serde2.teradata;

import com.google.common.io.BaseEncoding;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the data type DECIMAL for Teradata binary format.
 */
public class TestTeradataBinarySerdeForDecimal {

  private final TeradataBinarySerde serde = new TeradataBinarySerde();
  private final Properties props = new Properties();

  @Before
  public void setUp() throws Exception {
    props.setProperty(serdeConstants.LIST_COLUMNS, "TD_DECIMAL");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "decimal(9,5)");

    serde.initialize(null, props, props);
  }

  @Test
  public void testPositiveFraction() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("0064000000"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("0.001".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testNegativeFraction() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("009cffffff"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("-0.001".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testPositiveNumber1() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("00a0860100"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("1".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testNegativeNumber1() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("006079feff"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("-1".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testPositiveNumber2() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("0080969800"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("100".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testNegativeNumber2() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("000065c4e0"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertTrue("-5240".equals(((HiveDecimalWritable) row.get(0)).getHiveDecimal().toString()));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }
}
