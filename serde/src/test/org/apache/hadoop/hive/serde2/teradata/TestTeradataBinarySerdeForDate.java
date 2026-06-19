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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the data type DATE for Teradata binary format.
 */
public class TestTeradataBinarySerdeForDate {

  private final TeradataBinarySerde serde = new TeradataBinarySerde();
  private final Properties props = new Properties();

  @Before
  public void setUp() throws Exception {
    props.setProperty(serdeConstants.LIST_COLUMNS, "TD_DATE");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "date");
    serde.initialize(null, props, null);
  }

  @Test
  public void testTimestampBefore1900() throws Exception {

    //0060-01-01
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("00653de7fe"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Date ts = ((DateWritableV2) row.get(0)).get();
    Assert.assertEquals(ts.getYear(), 60);
    Assert.assertEquals(ts.getMonth(), 1);
    Assert.assertEquals(ts.getDay(), 1);

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  @Test
  public void testTimestampAfter1900() throws Exception {

    //9999-01-01
    BytesWritable in = new BytesWritable(BaseEncoding.base16().lowerCase().decode("0095cfd304"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Date ts = ((DateWritableV2) row.get(0)).get();
    Assert.assertEquals(ts.getYear(), 9999);
    Assert.assertEquals(ts.getMonth(), 1);
    Assert.assertEquals(ts.getDay(), 1);

    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }
}
