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
package org.apache.hcatalog.data;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.HCatRecordObjectInspectorFactory;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatSchemaUtils;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestLazyHCatRecord extends TestCase{

  private final int INT_CONST = 789;
  private final long LONG_CONST = 5000000000L;
  private final double DOUBLE_CONST = 3.141592654;
  private final String STRING_CONST = "hello world";


  public void testGet() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    assertEquals(INT_CONST, ((Integer)r.get(0)).intValue());
    assertEquals(LONG_CONST, ((Long)r.get(1)).longValue());
    assertEquals(DOUBLE_CONST, ((Double)r.get(2)).doubleValue());
    assertEquals(STRING_CONST, (String)r.get(3));
  }

  public void testGetWithName() throws Exception {
    TypeInfo ti = getTypeInfo();
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector(ti));
    HCatSchema schema = HCatSchemaUtils.getHCatSchema(ti)
                                          .get(0).getStructSubSchema();
    assertEquals(INT_CONST, ((Integer)r.get("an_int", schema)).intValue());
    assertEquals(LONG_CONST, ((Long)r.get("a_long", schema)).longValue());
    assertEquals(DOUBLE_CONST, ((Double)r.get("a_double", schema)).doubleValue());
    assertEquals(STRING_CONST, (String)r.get("a_string", schema));
  }

  public void testGetAll() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    List<Object> list = r.getAll();
    assertEquals(INT_CONST, ((Integer)list.get(0)).intValue());
    assertEquals(LONG_CONST, ((Long)list.get(1)).longValue());
    assertEquals(DOUBLE_CONST, ((Double)list.get(2)).doubleValue());
    assertEquals(STRING_CONST, (String)list.get(3));
  }

  public void testSet() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.set(3, "Mary had a little lamb");
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testSize() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    assertEquals(4, r.size());
  }

  public void testReadFields() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.readFields(null);
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testWrite() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.write(null);
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testSetWithName() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.set("fred", null, "bob");
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testRemove() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.remove(0);
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testCopy() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector());
    boolean sawException = false;
    try {
      r.copy(null);
    } catch (UnsupportedOperationException uoe) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  public void testGetWritable() throws Exception {
    HCatRecord r = new LazyHCatRecord(getHCatRecord(), getObjectInspector()).getWritable();
    assertEquals(INT_CONST, ((Integer)r.get(0)).intValue());
    assertEquals(LONG_CONST, ((Long)r.get(1)).longValue());
    assertEquals(DOUBLE_CONST, ((Double)r.get(2)).doubleValue());
    assertEquals(STRING_CONST, (String)r.get(3));
    assertEquals("org.apache.hcatalog.data.DefaultHCatRecord", r.getClass().getName());
  }


  private HCatRecord getHCatRecord() throws Exception {

    List<Object> rec_1 = new ArrayList<Object>(4);
    rec_1.add( new Integer(INT_CONST));
    rec_1.add( new Long(LONG_CONST));
    rec_1.add( new Double(DOUBLE_CONST));
    rec_1.add( new String(STRING_CONST));

    return new DefaultHCatRecord(rec_1);
  }

  private TypeInfo getTypeInfo() throws Exception {
    List<String> names = new ArrayList<String>(4);
    names.add("an_int");
    names.add("a_long");
    names.add("a_double");
    names.add("a_string");

    List<TypeInfo> tis = new ArrayList<TypeInfo>(4);
    tis.add(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    tis.add(TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    tis.add(TypeInfoFactory.getPrimitiveTypeInfo("double"));
    tis.add(TypeInfoFactory.getPrimitiveTypeInfo("string"));

    return TypeInfoFactory.getStructTypeInfo(names, tis);

  }

  private ObjectInspector getObjectInspector(TypeInfo ti) throws Exception {
    return HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(
        (StructTypeInfo)ti);
  }

  private ObjectInspector getObjectInspector() throws Exception {
    return HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector(
          (StructTypeInfo)getTypeInfo());
  }
}
