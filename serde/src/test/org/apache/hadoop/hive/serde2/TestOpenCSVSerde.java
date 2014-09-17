package org.apache.hadoop.hive.serde2;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
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

public class TestOpenCSVSerde {
  private final OpenCSVSerde csv = new OpenCSVSerde();
  private final Properties props = new Properties();

  @Before
  public void setup() throws Exception {
    props.setProperty(serdeConstants.LIST_COLUMNS, "a,b,c");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");
  }

  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);
    final Text in = new Text("hello,\"yes, okay\",1");

    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
  }


  @Test
  public void testDeserializeCustomSeparators() throws Exception {
    props.setProperty(OpenCSVSerde.SEPARATORCHAR, "\t");
    props.setProperty(OpenCSVSerde.QUOTECHAR, "'");

    csv.initialize(null, props);

    final Text in = new Text("hello\t'yes\tokay'\t1");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes\tokay", row.get(1));
    assertEquals("1", row.get(2));
  }

  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.setProperty(OpenCSVSerde.QUOTECHAR, "'");
    props.setProperty(OpenCSVSerde.ESCAPECHAR, "\\");

    csv.initialize(null, props);

    final Text in = new Text("hello,'yes\\'okay',1");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes'okay", row.get(1));
    assertEquals("1", row.get(2));
  }
}
