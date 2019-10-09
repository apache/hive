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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link RegexTextSerDe}.
 */
public class TestRegexTextSerDe {

  private RegexTextSerDe serde;

  @Before
  public void setup() {
    this.serde = new RegexTextSerDe();
  }

  /**
   * RegexSerDe does not support serialization.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testSerialize() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "c1");
    props.setProperty(RegexTextSerDe.INPUT_REGEX, "(.*)");

    this.serde.initialize(null, props);

    this.serde.serialize("test",
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeDefault() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "c1,c2");
    props.setProperty(RegexTextSerDe.INPUT_REGEX, "(.*),(.*)");

    this.serde.initialize(null, props);

    final Text testText = new Text("A,B");

    final List<String> result = (List<String>) this.serde.deserialize(testText);

    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("A", result.get(0));
    Assert.assertEquals("B", result.get(1));
  }

  @Test(expected = SerDeException.class)
  public void testDeserializeExtraField() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "c1,c2");
    props.setProperty(RegexTextSerDe.INPUT_REGEX, "(A)");

    this.serde.initialize(null, props);

    // 3 values, 2 columns
    final Text testText = new Text("AAA");

    this.serde.deserialize(testText);
  }

  @Test(expected = SerDeException.class)
  public void testDeserializeMissingField() throws SerDeException {
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "c1,c2");
    props.setProperty(RegexTextSerDe.INPUT_REGEX, "(.*),(.*)");

    this.serde.initialize(null, props);

    final Text testText = new Text("A");

    this.serde.deserialize(testText);
  }
}
