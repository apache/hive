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
import junit.framework.TestCase;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Assert;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Test the data type TIMESTAMP for Teradata binary format.
 */
public class TestTeradataBinarySerdeForTimeStamp extends TestCase {

  private final TeradataBinarySerde serde = new TeradataBinarySerde();
  private final Properties props = new Properties();

  protected void setUp() throws Exception {
    props.setProperty(serdeConstants.LIST_COLUMNS, "TD_TIMESTAMP");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "timestamp");
  }

  public void testTimestampPrecision6() throws Exception {
    props.setProperty(TeradataBinarySerde.TD_TIMESTAMP_PRECISION, "6");
    serde.initialize(null, props);

    //2012-10-01 12:00:00.110000
    BytesWritable in = new BytesWritable(
        BaseEncoding.base16().lowerCase().decode("00323031322d31302d30312031323a30303a30302e313130303030"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Timestamp ts = ((TimestampWritable) row.get(0)).getTimestamp();
    Assert.assertEquals("2012-10-01 12:00:00.11", ts.toString());


    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  public void testTimestampPrecision0() throws Exception {
    props.setProperty(TeradataBinarySerde.TD_TIMESTAMP_PRECISION, "0");
    serde.initialize(null, props);

    //2012-10-01 12:00:00
    BytesWritable in =
        new BytesWritable(BaseEncoding.base16().lowerCase().decode("00323031322d31302d30312031323a30303a3030"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Timestamp ts = ((TimestampWritable) row.get(0)).getTimestamp();
    Assert.assertEquals("2012-10-01 12:00:00.0", ts.toString());


    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }

  public void testTimestampPrecision3() throws Exception {
    props.setProperty(TeradataBinarySerde.TD_TIMESTAMP_PRECISION, "3");
    serde.initialize(null, props);

    //2012-10-01 12:00:00.345
    BytesWritable in =
        new BytesWritable(BaseEncoding.base16().lowerCase().decode("00323031322d31302d30312031323a30303a30302e333435"));

    List<Object> row = (List<Object>) serde.deserialize(in);
    Timestamp ts = ((TimestampWritable) row.get(0)).getTimestamp();
    Assert.assertEquals("2012-10-01 12:00:00.345", ts.toString());


    BytesWritable res = (BytesWritable) serde.serialize(row, serde.getObjectInspector());
    Assert.assertTrue(Arrays.equals(in.copyBytes(), res.copyBytes()));
  }
}
