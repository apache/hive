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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeTypei64.
 *
 */
public class DynamicSerDeTypei64 extends DynamicSerDeTypeBase {

  @Override
  public Class getRealType() {
    return Long.valueOf(0).getClass();
  }

  // production is: i64

  public DynamicSerDeTypei64(int i) {
    super(i);
  }

  public DynamicSerDeTypei64(thrift_grammar p, int i) {
    super(p, i);
  }

  @Override
  public String toString() {
    return "i64";
  }

  @Override
  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    long val = iprot.readI64();
    if (val == 0
        && iprot instanceof org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol
        && ((org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol) iprot)
        .lastPrimitiveWasNull()) {
      return null;
    }
    return Long.valueOf(val);
  }

  @Override
  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException {
    LongObjectInspector poi = (LongObjectInspector) oi;
    oprot.writeI64(poi.get(o));
  }

  @Override
  public byte getType() {
    return TType.I64;
  }
}
