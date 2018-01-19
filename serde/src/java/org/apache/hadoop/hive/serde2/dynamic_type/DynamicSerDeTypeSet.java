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

package org.apache.hadoop.hive.serde2.dynamic_type;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeTypeSet.
 *
 */
public class DynamicSerDeTypeSet extends DynamicSerDeTypeBase {

  // production is: set<FieldType()>

  private static final int FD_TYPE = 0;

  public DynamicSerDeTypeSet(int i) {
    super(i);
  }

  public DynamicSerDeTypeSet(thrift_grammar p, int i) {
    super(p, i);
  }

  // returns Set<?>
  @Override
  public Class getRealType() {
    try {
      Class c = getElementType().getRealType();
      Object o = c.newInstance();
      Set<?> l = Collections.singleton(o);
      return l.getClass();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public DynamicSerDeTypeBase getElementType() {
    return ((DynamicSerDeFieldType) jjtGetChild(FD_TYPE)).getMyType();
  }

  @Override
  public String toString() {
    return "set<" + getElementType().toString() + ">";
  }

  @Override
  public byte getType() {
    return TType.SET;
  }

  /**
   * NOTE: Set is not supported by Hive yet.
   */
  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    TSet theset = iprot.readSetBegin();
    if (theset == null) {
      return null;
    }
    Set<Object> result;
    if (reuse != null) {
      result = (Set<Object>) reuse;
      result.clear();
    } else {
      result = new HashSet<Object>();
    }
    for (int i = 0; i < theset.size; i++) {
      Object elem = getElementType().deserialize(null, iprot);
      result.add(elem);
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not
    // get too crazy
    iprot.readSetEnd();
    return result;
  }

  /**
   * NOTE: Set is not supported by Hive yet. The code uses ListObjectInspector
   * right now. We need to change it to SetObjectInspector when that is done.
   */
  TSet tset = null;

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException {

    ListObjectInspector loi = (ListObjectInspector) oi;

    Set<Object> set = (Set<Object>) o;
    DynamicSerDeTypeBase mt = getElementType();
    tset = new TSet(mt.getType(), set.size());
    oprot.writeSetBegin(tset);
    for (Object element : set) {
      mt.serialize(element, loi.getListElementObjectInspector(), oprot);
    }
    // in theory, the below call isn't needed in non thrift_mode, but let's not
    // get too crazy
    oprot.writeSetEnd();
  }
}
