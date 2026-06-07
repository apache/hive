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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class TestBtreeObjectInspector {

  @Test
  public void testBtreeObjectInspector() {
    List<String> fieldNames = new ArrayList<>();
    fieldNames.add("key");
    fieldNames.add("vs");

    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(intTypeInfo);

    ListTypeInfo lti = new ListTypeInfo();
    StructTypeInfo eti = new StructTypeInfo();

    List<String> structFieldNames = new ArrayList<>();
    structFieldNames.add("v0");
    structFieldNames.add("v1");
    structFieldNames.add("v2");

    List<TypeInfo> structTypeInfos = new ArrayList<>();
    structTypeInfos.add(longTypeInfo);
    structTypeInfos.add(intTypeInfo);
    structTypeInfos.add(stringTypeInfo);
    eti.setAllStructFieldNames(structFieldNames);
    eti.setAllStructFieldTypeInfos(structTypeInfos);
    lti.setListElementTypeInfo(eti);

    typeInfos.add(lti);

    StructTypeInfo sti = new StructTypeInfo();
    sti.setAllStructFieldNames(fieldNames);
    sti.setAllStructFieldTypeInfos(typeInfos);
    ObjectInspector oi = new BtreeObjectInspector(sti);
    String typeName = oi.getTypeName();
    System.out.println(typeName);
  }
}
