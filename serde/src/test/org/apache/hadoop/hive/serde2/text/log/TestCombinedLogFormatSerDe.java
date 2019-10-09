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
package org.apache.hadoop.hive.serde2.text.log;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

/**
 * Test suite for {@link CombinedLogFormatSerDe}.
 */
public class TestCombinedLogFormatSerDe {

  private CombinedLogFormatSerDe serde;

  @Before
  public void setup() {
    this.serde = new CombinedLogFormatSerDe();
  }

  /**
   * CommonLogFormatSerDe SerDe does not support serialization.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testSerialize() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "c1,c2,c3,c4,c5,c6,c7,c8,c9");

    this.serde.initialize(null, props);

    this.serde.serialize("test",
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  /**
   * CommonLogFormatSerDe SerDe requires a predefined schema that matches the
   * log format.
   */
  @Test(expected = SerDeException.class)
  public void testColumnCount() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "c1,c2,c3,c4,c5,c6,c7,c8");

    this.serde.initialize(null, props);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeDefault() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS,
        "c1,c2,c3,c4,c5,c6,c7,c8,c9");

    this.serde.initialize(null, props);

    final Text testText =
        new Text("127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "
            + "\"GET /pb.gif HTTP/1.0\" 200 2326 "
            + "\"http://www.example.com/start.html\" "
            + "\"Mozilla/4.08 [en] (Win98; I ;Nav)\"");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(9, result.size());
    Assert.assertEquals("127.0.0.1", result.get(0));
    Assert.assertNull(result.get(1));
    Assert.assertEquals("frank", result.get(2));
    Assert.assertEquals("10/Oct/2000:13:55:36 -0700", result.get(3));
    Assert.assertEquals("GET /pb.gif HTTP/1.0", result.get(4));
    Assert.assertEquals("200", result.get(5));
    Assert.assertEquals("2326", result.get(6));
    Assert.assertEquals("http://www.example.com/start.html", result.get(7));
    Assert.assertEquals("Mozilla/4.08 [en] (Win98; I ;Nav)", result.get(8));
  }
}
