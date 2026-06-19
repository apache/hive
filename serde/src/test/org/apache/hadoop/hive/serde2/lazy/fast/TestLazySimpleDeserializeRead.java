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
package org.apache.hadoop.hive.serde2.lazy.fast;



import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Unit tests for LazySimpleDeserializeRead.
 *
 */
public class TestLazySimpleDeserializeRead {

  /**
   * Test for escaping.
   *
   */
  @Test
  public void testEscaping() throws Exception {
    HiveConf hconf = new HiveConf();

    // set the escaping related properties
    Properties props = new Properties();
    props.setProperty(serdeConstants.FIELD_DELIM, "|");
    props.setProperty(serdeConstants.ESCAPE_CHAR, "\\");
    props.setProperty(serdeConstants.SERIALIZATION_ESCAPE_CRLF, "true");

    LazySerDeParameters lazyParams =
        new LazySerDeParameters(hconf, props,
            LazySimpleSerDe.class.getName());

    TypeInfo[] typeInfos = new TypeInfo[2];
    typeInfos[0] = TypeInfoFactory.getPrimitiveTypeInfo("string");
    typeInfos[1] = TypeInfoFactory.getPrimitiveTypeInfo("string");

    LazySimpleDeserializeRead deserializeRead =
        new LazySimpleDeserializeRead(typeInfos, null, true, lazyParams);

    // set and parse the row
    String s = "This\\nis\\rthe first\\r\\nmulti-line field\\n|field1-2";
    Text row = new Text(s.getBytes("UTF-8"));
    deserializeRead.set(row.getBytes(), 0, row.getLength());

    assertTrue(deserializeRead.readNextField());
    assertTrue(deserializeRead.currentExternalBufferNeeded);

    int externalBufferLen = deserializeRead.currentExternalBufferNeededLen;
    assertEquals("Wrong external buffer length", externalBufferLen, 36);

    byte[] externalBuffer = new byte[externalBufferLen];
    deserializeRead.copyToExternalBuffer(externalBuffer, 0);

    Text field = new Text();
    field.set(externalBuffer, 0, externalBufferLen);

    String f = "This\nis\rthe first\r\nmulti-line field\n";
    Text escaped = new Text(f.getBytes("UTF-8"));

    assertTrue("The escaped result is incorrect", field.compareTo(escaped) == 0);
  }
}
