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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestOrcStruct {

  @Test
  public void testStruct() throws Exception {
    OrcStruct st1 = new OrcStruct(4);
    OrcStruct st2 = new OrcStruct(4);
    OrcStruct st3 = new OrcStruct(3);
    st1.setFieldValue(0, "hop");
    st1.setFieldValue(1, "on");
    st1.setFieldValue(2, "pop");
    st1.setFieldValue(3, 42);
    assertEquals(false, st1.equals(null));
    st2.setFieldValue(0, "hop");
    st2.setFieldValue(1, "on");
    st2.setFieldValue(2, "pop");
    st2.setFieldValue(3, 42);
    assertEquals(st1, st2);
    st3.setFieldValue(0, "hop");
    st3.setFieldValue(1, "on");
    st3.setFieldValue(2, "pop");
    assertEquals(false, st1.equals(st3));
    assertEquals(11241, st1.hashCode());
    assertEquals(st1.hashCode(), st2.hashCode());
    assertEquals(11204, st3.hashCode());
    assertEquals("{hop, on, pop, 42}", st1.toString());
    st1.setFieldValue(3, null);
    assertEquals(false, st1.equals(st2));
    assertEquals(false, st2.equals(st1));
    st2.setFieldValue(3, null);
    assertEquals(st1, st2);
  }

  @Test
  public void testInspectorFromTypeInfo() throws Exception {
    TypeInfo typeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString("struct<c1:boolean,c2:tinyint" +
            ",c3:smallint,c4:int,c5:bigint,c6:float,c7:double,c8:binary," +
            "c9:string,c10:struct<c1:int>,c11:map<int,int>,c12:uniontype<int>" +
            ",c13:array<timestamp>>");
    StructObjectInspector inspector = (StructObjectInspector)
        OrcStruct.createObjectInspector(typeInfo);
    assertEquals("struct<c1:boolean,c2:tinyint,c3:smallint,c4:int,c5:" +
        "bigint,c6:float,c7:double,c8:binary,c9:string,c10:struct<" +
        "c1:int>,c11:map<int,int>,c12:uniontype<int>,c13:array<timestamp>>",
        inspector.getTypeName());
    assertEquals(null,
        inspector.getAllStructFieldRefs().get(0).getFieldComment());
    assertEquals(null, inspector.getStructFieldRef("UNKNOWN"));
    OrcStruct s1 = new OrcStruct(13);
    for(int i=0; i < 13; ++i) {
      s1.setFieldValue(i, i);
    }

    List<Object> list = new ArrayList<Object>();
    list.addAll(Arrays.asList(0,1,2,3,4,5,6,7,8,9,10,11,12));
    assertEquals(list, inspector.getStructFieldsDataAsList(s1));
    ListObjectInspector listOI = (ListObjectInspector)
        inspector.getAllStructFieldRefs().get(12).getFieldObjectInspector();
    assertEquals(ObjectInspector.Category.LIST, listOI.getCategory());
    assertEquals(10, listOI.getListElement(list, 10));
    assertEquals(null, listOI.getListElement(list, -1));
    assertEquals(null, listOI.getListElement(list, 13));
    assertEquals(13, listOI.getListLength(list));

    Map<Integer, Integer> map = new HashMap<Integer,Integer>();
    map.put(1,2);
    map.put(2,4);
    map.put(3,6);
    MapObjectInspector mapOI = (MapObjectInspector)
        inspector.getAllStructFieldRefs().get(10).getFieldObjectInspector();
    assertEquals(3, mapOI.getMapSize(map));
    assertEquals(4, mapOI.getMapValueElement(map, 2));
  }

  @Test
  public void testUnion() throws Exception {
    OrcUnion un1 = new OrcUnion();
    OrcUnion un2 = new OrcUnion();
    un1.set((byte) 0, "hi");
    un2.set((byte) 0, "hi");
    assertEquals(un1, un2);
    assertEquals(un1.hashCode(), un2.hashCode());
    un2.set((byte) 0, null);
    assertEquals(false, un1.equals(un2));
    assertEquals(false, un2.equals(un1));
    un1.set((byte) 0, null);
    assertEquals(un1, un2);
    un2.set((byte) 0, "hi");
    un1.set((byte) 1, "hi");
    assertEquals(false, un1.equals(un2));
    assertEquals(false, un1.hashCode() == un2.hashCode());
    un2.set((byte) 1, "byte");
    assertEquals(false, un1.equals(un2));
    assertEquals("union(1, hi)", un1.toString());
    assertEquals(false, un1.equals(null));
  }
}
