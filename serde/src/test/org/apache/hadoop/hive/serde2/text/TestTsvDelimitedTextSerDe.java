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
package org.apache.hadoop.hive.serde2.text;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link TsvDelimitedTextSerDe}.
 */
public class TestTsvDelimitedTextSerDe {

  private TsvDelimitedTextSerDe serde;

  @Before
  public void setup() {
    this.serde = new TsvDelimitedTextSerDe();
  }

  @Test
  public void testSerialize() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");

    this.serde.initialize(null, props);

    ObjectInspector oi = serde.getObjectInspector();

    Object testStructure = Arrays.asList(new Text("giraffe"),
        new DoubleWritable(3.21), new IntWritable(20),
        new BooleanWritable(false), new TimestampWritable(
            Timestamp.from(Instant.parse("2014-10-14T12:34:56.02Z"))));

    final TimeZone previousTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try {
      final String result = this.serde.doSerialize(testStructure, oi);
      Assert.assertEquals("giraffe\t3.21\t20\tfalse\t2014-10-14 12:34:56.02",
          result);
    } finally {
      TimeZone.setDefault(previousTimeZone);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeDefault() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");

    this.serde.initialize(null, props);

    final Text testText =
        new Text("giraffe\t3.21\t20\tfalse\t2014-10-14 12:34:56.789");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals("giraffe", result.get(0));
    Assert.assertEquals("3.21", result.get(1));
    Assert.assertEquals("20", result.get(2));
    Assert.assertEquals("false", result.get(3));
    Assert.assertEquals("2014-10-14 12:34:56.789", result.get(4));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeExtraField() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");

    this.serde.initialize(null, props);

    final Text testText =
        new Text("giraffe\t3.21\t20\tfalse\t2014-10-14 12:34:56.789\tXXX");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals("giraffe", result.get(0));
    Assert.assertEquals("3.21", result.get(1));
    Assert.assertEquals("20", result.get(2));
    Assert.assertEquals("false", result.get(3));
    Assert.assertEquals("2014-10-14 12:34:56.789\tXXX", result.get(4));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeMissingField() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");

    this.serde.initialize(null, props);

    final Text testText = new Text("giraffe\t3.21\t20\tfalse");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals("giraffe", result.get(0));
    Assert.assertEquals("3.21", result.get(1));
    Assert.assertEquals("20", result.get(2));
    Assert.assertEquals("false", result.get(3));
    Assert.assertNull(result.get(4));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeEmptyRow() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");

    this.serde.initialize(null, props);

    final Text testText = new Text("");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(5, result.size());
    Assert.assertNull(result.get(0));
    Assert.assertNull(result.get(1));
    Assert.assertNull(result.get(2));
    Assert.assertNull(result.get(3));
    Assert.assertNull(result.get(4));
  }

  @Test(expected = SerDeException.class)
  public void testDeserializeEmptyRowRaiseException() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");
    props.setProperty(AbstractTextSerDe.IGNORE_EMPTY_LINES, "false");

    this.serde.initialize(null, props);

    final Text testText = new Text("");

    this.serde.deserialize(testText);
  }

  @Test(expected = SerDeException.class)
  public void testDeserializeExtraFieldRowRaiseException()
      throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");
    props.setProperty(AbstractDelimitedTextSerDe.STRICT_FIELDS_COUNT, "true");

    this.serde.initialize(null, props);

    final Text testText =
        new Text("giraffe\t3.21\t20\tfalse\t2014-10-14 12:34:56.789\tXXX");

    this.serde.deserialize(testText);
  }

  @Test(expected = SerDeException.class)
  public void testDeserializeMissingFieldRowRaiseException()
      throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "name,height,weight,endangered,born");
    props.setProperty(AbstractDelimitedTextSerDe.STRICT_FIELDS_COUNT, "true");

    this.serde.initialize(null, props);

    final Text testText = new Text("giraffe\t3.21\t20\tfalse");

    this.serde.deserialize(testText);
  }
}
