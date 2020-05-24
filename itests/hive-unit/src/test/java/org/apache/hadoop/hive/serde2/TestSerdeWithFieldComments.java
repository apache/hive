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
package org.apache.hadoop.hive.serde2;


import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestSerdeWithFieldComments.
 */
public class TestSerdeWithFieldComments {

  private StructField mockedStructField(String name, String oiTypeName,
                                        String comment) {
    StructField m = mock(StructField.class);
    when(m.getFieldName()).thenReturn(name);

    ObjectInspector oi = mock(ObjectInspector.class);
    when(oi.getTypeName()).thenReturn(oiTypeName);

    when(m.getFieldObjectInspector()).thenReturn(oi);
    when(m.getFieldComment()).thenReturn(comment);

    return m;
  }

  @Test
  public void testFieldComments() throws MetaException, SerDeException {
    StructObjectInspector mockSOI = mock(StructObjectInspector.class);
    when(mockSOI.getCategory()).thenReturn(ObjectInspector.Category.STRUCT);
    List fieldRefs = new ArrayList<StructField>();
    // Add field with a comment...
    fieldRefs.add(mockedStructField("first", "type name 1", "this is a comment"));
    // ... and one without
    fieldRefs.add(mockedStructField("second", "type name 2", null));

    when(mockSOI.getAllStructFieldRefs()).thenReturn(fieldRefs);

    Deserializer mockDe = mock(Deserializer.class);
    when(mockDe.getObjectInspector()).thenReturn(mockSOI);
    List<FieldSchema> result =
        HiveMetaStoreUtils.getFieldsFromDeserializer("testTable", mockDe);

    assertEquals(2, result.size());
    assertEquals("first", result.get(0).getName());
    assertEquals("this is a comment", result.get(0).getComment());
    assertEquals("second", result.get(1).getName());
    assertEquals("from deserializer", result.get(1).getComment());
  }
}
