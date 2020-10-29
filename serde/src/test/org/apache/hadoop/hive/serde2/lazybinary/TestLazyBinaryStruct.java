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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Properties;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestLazyBinaryStruct extends TestCase {

  @Test
  public void testEmptyStruct() {
    LazyBinaryStructObjectInspector oi = LazyBinaryObjectInspectorFactory
        .getLazyBinaryStructObjectInspector(new ArrayList<>(), new ArrayList<>());

    ByteArrayRef byteRef = new ByteArrayRef();
    byteRef.setData(new byte[]{0});

    LazyBinaryStruct data = (LazyBinaryStruct) LazyBinaryFactory.createLazyBinaryObject(oi);
    data.init(byteRef, 0, 0);

    assertEquals(data.getRawDataSerializedSize(), 0);
  }
  
  @Test
  public void testEmptyStructWithSerde() throws SerDeException {
    LazyBinaryStructObjectInspector oi = LazyBinaryObjectInspectorFactory
        .getLazyBinaryStructObjectInspector(new ArrayList<>(), new ArrayList<>());
    StandardStructObjectInspector standardOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(new ArrayList<>(), new ArrayList<>());
    Properties schema = new Properties();
    schema.setProperty(serdeConstants.LIST_COLUMNS, "col0");
    schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, "struct<>");

    LazyBinarySerDe serde = new LazyBinarySerDe();
    serde.initialize(new Configuration(), schema, null);
    Writable writable = serde.serialize(standardOI.create(), standardOI);
    Object out = serde.deserialize(writable);
    assertNull(oi.getStructFieldsDataAsList(out));
  }
}