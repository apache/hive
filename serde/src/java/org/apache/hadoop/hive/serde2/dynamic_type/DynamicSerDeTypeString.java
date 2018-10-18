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

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.WriteTextProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeTypeString.
 *
 */
public class DynamicSerDeTypeString extends DynamicSerDeTypeBase {

  // production is: string

  public DynamicSerDeTypeString(int i) {
    super(i);
  }

  public DynamicSerDeTypeString(thrift_grammar p, int i) {
    super(p, i);
  }

  @Override
  public Class getRealType() {
    return java.lang.String.class;
  }

  @Override
  public String toString() {
    return "string";
  }

  public String deserialize(TProtocol iprot) throws SerDeException, TException,
      IllegalAccessException {
    return iprot.readString();
  }

  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    return iprot.readString();
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException {
    StringObjectInspector poi = (StringObjectInspector) oi;
    if (oprot instanceof WriteTextProtocol) {
      ((WriteTextProtocol) oprot).writeText(poi.getPrimitiveWritableObject(o));
    } else {
      oprot.writeString(poi.getPrimitiveJavaObject(o));
    }
  }

  @Override
  public byte getType() {
    return TType.STRING;
  }
}
