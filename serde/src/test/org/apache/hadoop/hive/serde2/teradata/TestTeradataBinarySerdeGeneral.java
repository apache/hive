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
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

/**
 * Test all the data types supported for Teradata Binary Format.
 */
public class TestTeradataBinarySerdeGeneral {

  private final TeradataBinarySerde serde = new TeradataBinarySerde();
  private final Properties props = new Properties();

  @Before
  public void setUp() throws Exception {
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "TD_CHAR, TD_VARCHAR, TD_BIGINT, TD_INT, TD_SMALLINT, TD_BYTEINT, "
            + "TD_FLOAT,TD_DECIMAL,TD_DATE, TD_TIMESTAMP, TD_VARBYTE");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        "char(3),varchar(100),bigint,int,smallint,tinyint,double,decimal(31,30),date,timestamp,binary");

    serde.initialize(null, props);
  }

  @Test
  public void testDeserializeAndSerialize() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode(
        "00004e6f762020202020201b006120646179203d2031312f31312f31312020202020202020203435ec10000000000000c5feffff"
            + "7707010000000000002a40ef2b3dab0d14e6531c8908a72700000007b20100313931312d31312d31312031393a32303a32312e34"
            + "33333230301b00746573743a20202020202020343333322020202020202020333135"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertEquals("Nov", ((HiveCharWritable) row.get(0)).toString());
    Assert.assertEquals("a day = 11/11/11         45", ((HiveVarcharWritable) row.get(1)).toString());
    Assert.assertEquals(4332L, ((LongWritable) row.get(2)).get());
    Assert.assertEquals(-315, ((IntWritable) row.get(3)).get());
    Assert.assertEquals((short) 1911, ((ShortWritable) row.get(4)).get());
    Assert.assertEquals((byte) 1, ((ByteWritable) row.get(5)).get());
    Assert.assertEquals((double) 13, ((DoubleWritable) row.get(6)).get(), 0);
    Assert.assertEquals(30, ((HiveDecimalWritable) row.get(7)).getScale());
    Assert.assertEquals((double) 3.141592653589793238462643383279,
        ((HiveDecimalWritable) row.get(7)).getHiveDecimal().doubleValue(), 0);
    Assert.assertEquals("1911-11-11", ((DateWritableV2) row.get(8)).toString());
    Assert.assertEquals("1911-11-11 19:20:21.4332", ((TimestampWritableV2) row.get(9)).toString());
    Assert.assertEquals(27, ((BytesWritable) row.get(10)).getLength());

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testDeserializeAndSerializeWithNull() throws Exception {
    //null bitmap: 0160 -> 00000001 01100000, 7th, 9th, 10th is null
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode(
        "01604d61722020202020201b006120646179203d2031332f30332f303820202020202020202020397ca10000000000004300000"
            + "0dd0700000000000048834000000000000000000000000000000000443f110020202020202020202020202020202020202020202"
            + "020202020200000"));
    List<Object> row = (List<Object>) serde.deserialize(in);

    Assert.assertEquals("Mar", ((HiveCharWritable) row.get(0)).toString());
    Assert.assertEquals(null, row.get(7));
    Assert.assertEquals(null, row.get(9));
    Assert.assertEquals(null, row.get(10));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testDeserializeAndSerializeAllNull() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode(
        "ffe0202020202020202020000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000020202020202020202020202020202020202020202020202020200000"));
    List<Object> row = (List<Object>) serde.deserialize(in);

    Assert.assertEquals(null, row.get(0));
    Assert.assertEquals(null, row.get(1));
    Assert.assertEquals(null, row.get(3));
    Assert.assertEquals(null, row.get(4));
    Assert.assertEquals(null, row.get(5));
    Assert.assertEquals(null, row.get(6));
    Assert.assertEquals(null, row.get(7));
    Assert.assertEquals(null, row.get(8));
    Assert.assertEquals(null, row.get(9));
    Assert.assertEquals(null, row.get(10));

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testDeserializeCorruptedRecord() throws Exception {
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode(
        "00004e6f762020202020201b006120646179203d2031312f31312f31312020202020202020203435ec10000000000000c5feff"
            + "ff7707010000000000002a40ef2b3dab0d14e6531c8908a72700000007b20100313931312d31312d31312031393a32303a32312"
            + "e3433333230301b00746573743a20202020202020343333322020202020202020333135ff"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Assert.assertEquals(null, row.get(0));
    Assert.assertEquals(null, row.get(3));
    Assert.assertEquals(null, row.get(10));
  }
}
