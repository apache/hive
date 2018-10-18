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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeTypeDouble.
 *
 */
public class DynamicSerDeTypeDouble extends DynamicSerDeTypeBase {

  // production is: double

  public DynamicSerDeTypeDouble(int i) {
    super(i);
  }

  public DynamicSerDeTypeDouble(thrift_grammar p, int i) {
    super(p, i);
  }

  @Override
  public String toString() {
    return "double";
  }

  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    double val = iprot.readDouble();
    if (val == 0
        && iprot instanceof org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol
        && ((org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol) iprot)
        .lastPrimitiveWasNull()) {
      return null;
    }
    return Double.valueOf(val);
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException {
    DoubleObjectInspector poi = (DoubleObjectInspector) oi;
    oprot.writeDouble(poi.get(o));
  }

  @Override
  public byte getType() {
    return TType.DOUBLE;
  }

  @Override
  public Class getRealType() {
    return java.lang.Double.class;
  }

  public Double getRealTypeInstance() {
    return Double.valueOf(0);
  }
}
